/*
 * Copyright (c) 2019. Alberto García-Raboso
 * Copyright (c) 2019. Alvaro Gomez Ramos
 * Copyright (c) 2019. Pablo López Gallego
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bbva.ebdm.ocelot.engines.spark

import scala.sys.process.stringToProcess

import cats.implicits.catsSyntaxValidatedIdBinCompat0
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.{ConfigLoader, ConfigOps, StringOps}
import com.bbva.ebdm.ocelot.templates.spark.SparkBaseApp

class SparkEngineTest extends FunSpec with LogSupport with Matchers with BeforeAndAfterAll {

  val config: Config = ConfigLoader.load(Array())
  val emptyConfig: Config = ConfigFactory.parseString("app.name=SparkServiceTest")

  describe("initialization") {
    it("should set the app name") {
      val env = SparkEngine.getEnv(config)
        env.conf.get("spark.app.name").validNec shouldBe
          config.at("app.name".path[String])
      }

    it("should set other configuration parameters") {
      val env = SparkEngine.getEnv(config)
        env.conf.get("spark.master").validNec shouldBe
          config.at("engines.spark.spark.master".path[String])
    }

    it("spark.master should be empty") {
      val newEnv = SparkEngine.getEnv(emptyConfig)
      newEnv.conf.get("spark.master").validNec.toString shouldBe
        "Valid(spark://172.26.0.9:7077)"
    }
  }

  describe("usage") {
    it("should create an empty DataFrame") {
        val env = SparkEngine.getEnv(config)
        env.emptyDataFrame.count shouldBe 0
    }

    it("should create a non-empty DataFrame") {
        val env = SparkEngine.getEnv(config)
        import env.implicits._
        val values = List(1, 2, 3, 4, 5)
        val df = values.toDF()
        df shouldBe a[DataFrame]
        df.count shouldBe 5
    }
  }

  describe("Build class Service") {
      it("Create Database") {
        val env = SparkEngine.getEnv(config)
        env.sql("CREATE DATABASE IF NOT EXISTS prueba")
        env.sql("SHOW DATABASES")
          .collect()
          .map(_ (0))
          .contains("prueba") shouldBe true
    }

    it("Create table") {
      val env = SparkEngine.getEnv(config)
        env.sql("create table if not exists prueba.prueba_test (uno String, dos String)")
        env.sql("show tables in prueba")
          .select("tableName")
          .collect()
          .map(_ (0))
          .contains("prueba_test") shouldBe true
    }

    it("Table exist") {
      val env = SparkEngine.getEnv(config)
        env.sql("select * from prueba.prueba_test")
          .columns shouldBe Array("uno", "dos")
    }

    it("Drop table") {
      val env = SparkEngine.getEnv(config)
        env.sql("drop table if exists prueba.prueba_test")
        val dropTabla = env.sql("show tables in prueba")
          .select("tableName")
          .collect()
        if (dropTabla.isEmpty) {
          dropTabla shouldBe Array()
        }
        else {
          dropTabla
            .map(_ (0))
            .contains("prueba_test") shouldBe false
        }
    }

    it("Drop Database") {
      val env = SparkEngine.getEnv(config)
        env.sql("DROP DATABASE IF EXISTS prueba")
        val dropDatabase = env.sql("SHOW DATABASES")
          .collect()
        if (dropDatabase.isEmpty) {
          dropDatabase shouldBe Array()
        }
        else {
          dropDatabase
            .map(_ (0))
            .contains("prueba") shouldBe false
        }
    }
  }

  // SparkBaseApp with 5 seconds stream execution time
  trait SparkBaseAppTestImp extends SparkBaseApp {
    override def process(inputMap: Map[String, DataFrame]): Map[String, DataFrame] = {
      inputMap("1234").count()
      Map("idOUT" -> env.emptyDataFrame)
    }
  }

  describe("Process no streaming") {
    it("Should execute with no streaming without exception") {
      SparkBaseApp.design(Array())
        .build[SparkBaseAppTestImp] { app: SparkBaseAppTestImp =>
          noException should be thrownBy app.exec()
        }
    }
  }

  describe("Process streaming 5 sec") {
    it("Should execute 5 seconds streaming app without exception") {
      SparkBaseApp.design(Array("-DexecutionTimeInMS=5000"))
        .build[SparkBaseAppTestImp] { app: SparkBaseAppTestImp =>
          noException should be thrownBy app.exec()
        }
    }
  }

  describe("Process unlimited streaming") {
    it("Should execute unlimited streaming app without exception") {
      SparkBaseApp.design(Array("-DexecutionTimeInMS=0"))
        .build[SparkBaseAppTestImp] { app: SparkBaseAppTestImp =>
          noException should be thrownBy app.exec()
        }
    }
  }

  // Get Docker ready
  val dockerName: String = "docker_engine_spark"
  val dockerName_net: String = s"${dockerName}_net"
  val ip: String = "172.26.0.9"
  val subnet: String = ip.substring(0, ip.lastIndexOf(".") + 1) + "0/16"
  val masterPort: String = "7077"
  val slavePort: String = "35261"
  val hdfsPort: String = "9000"
  val kafkaPort: String = "9092"
  val userHome: String =  System.getProperty("user.home")
  val homeVolume: String = userHome + "/Documentos/ocelotVolume/"
  val dockerVolume: String = "/opt/volume/"
  val dockerImage: String = "ocelot/test:1.0"

  override def afterAll(): Unit = {
    info("AfterAll!")
    info("\n--------Closing FileSistem-----------")
    info(FileSystem.closeAll())

    info("\n--------Docker stop-----------")
    val stopDocker = s"docker stop $dockerName".!!
    info(stopDocker)

    info("\n--------rm network-----------")
    val rmNetwork = s"docker network rm $dockerName_net".!!
    info(rmNetwork)
  }

  override def beforeAll(): Unit = {
    info("BeforeAll!")
    info("\n--------Create network-----------")
    val createNet: String = s"docker network create --subnet=$subnet $dockerName_net".!!
    info(createNet)

    info("\n--------Docker run-----------")
    val runDocker: String = (s"docker run --rm " +
      s"--network=$dockerName_net --ip=$ip " +
      s"--expose $masterPort --expose $slavePort --expose $hdfsPort " +
      s"--expose $kafkaPort --expose 2181 --expose 3306 --expose 9083 " +
      s"-v $homeVolume:$dockerVolume " +
      s"-d --name $dockerName $dockerImage").!!
    info(runDocker)

    info("\n--------Docker run script startup-----------")
    val runStartup: String = s"docker exec $dockerName /opt/script/startup.sh".!!
    info(runStartup)

    info("\n--------Test: Create files in HDFS -----------")
    val HelloPath: String = getClass.getResource("/hello.txt").getPath
    info(s"cp $HelloPath $homeVolume".!!)
    info(s"chmod 777 ${homeVolume}hello.txt".!!)
    info(s"docker exec $dockerName hdfs dfs -mkdir /tmp/hola/".!!)
    info(s"docker exec $dockerName hdfs dfs -put ${dockerVolume}hello.txt /tmp/hola/hello.txt".!!)
    info(s"docker exec $dockerName hdfs dfs -chmod 777 /tmp/hola/hello.txt".!!)
    info(s"rm ${homeVolume}hello.txt".!!)

  }
}
