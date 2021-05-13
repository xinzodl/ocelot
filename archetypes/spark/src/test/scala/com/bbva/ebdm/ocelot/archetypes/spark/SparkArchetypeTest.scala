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

package com.bbva.ebdm.ocelot.archetypes.spark

import scala.sys.process.stringToProcess

import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.ConfigLoader
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine
import com.bbva.ebdm.ocelot.templates.spark.SparkBaseApp

class SparkArchetypeTest extends FunSpec with Matchers with LogSupport with BeforeAndAfterAll {
  val config: Config = ConfigLoader.load(Array())
  implicit lazy val spark: SparkSession = SparkEngine.getEnv(config)

  describe("Pre process") {
    it("Should exist hdfs:///tmp/hello.txt") {
      spark.read.text("/tmp/hello.txt").collect() should
        contain(Row("prueba; prueba prueba;"))
    }

    it("Should exist hdfs:///tmp/world.txt") {
      spark.read.text("/tmp/world.txt").collect() should
        contain(Row("prueba; prueba prueba;"))
    }
  }

  describe("Process") {
    it("Should execute without exception") {
      SparkBaseApp.design(Array()).build[SparkArchetype] { app: SparkArchetype =>
        app.env.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://172.26.0.9:9000")
        app.exec()
      }
    }
  }

  describe("Post process") {
    it("Should create a txt in a folder in HDFS, with a name starting with part and one ocelot output using type = HDFS and subtype = Row") {
      spark.read.text("/tmp/testRow/part*").collect() shouldBe
        spark.read.text("/tmp/hello.txt").collect()
    }

    it("Should create a txt in a folder in HDFS, with a name starting with part and one ocelot output using type = HDFS, subtype = Row and all Row configurations") {
      spark.read.text("/tmp/testRowAllConf/part*.gz").collect() shouldBe
        spark.read.text("/tmp/hello.txt").collect()
    }

    it("Should create a txt in HDFS with one ocelot output using type = HDFS and subtype = String") {
      spark.read.text("/tmp/testString.txt").collect() shouldBe
        spark.read.text("/tmp/hello.txt").collect()
    }

    it("Deleted file should not exist") {
      val deletedFile = intercept[org.apache.spark.sql.AnalysisException] {
        spark.read.text("/tmp/world.txt")
      }.toString
      deletedFile.contains("Path does not exist:") shouldBe true

      deletedFile.contains("/tmp/world.txt") shouldBe true
    }

    it("Should add Rows in tableOutput") {
      spark.sql("select * from testdatabase.tableOutput").collect() should
        contain(Row("Pre2", null, 1234, 56, 78))

      spark.sql("select * from testdatabase.tableOutput").collect() should
        contain(Row("Pre", null, 2222, 33, 44))

      spark.sql("select * from testdatabase.tableOutput").count() shouldBe
        2
    }
  }

  val dockerName: String = "docker_template_spark_sql"
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
    val copyToDockerVolume: String = s"cp $HelloPath $homeVolume".!!
    val chmodHello = s"chmod 777 ${homeVolume}hello.txt".!!
    val createHDFSFile: String = s"docker exec $dockerName hdfs dfs -copyFromLocal ${dockerVolume}hello.txt /tmp/hello.txt".!!
    val chmodHDFSFile: String = s"docker exec $dockerName hdfs dfs -chmod 777 /tmp/hello.txt".!!
    val createHDFSFile2: String = s"docker exec $dockerName hdfs dfs -copyFromLocal ${dockerVolume}hello.txt /tmp/world.txt".!!
    val chmodHDFSFile2: String = s"docker exec $dockerName hdfs dfs -chmod 777 /tmp/world.txt".!!
    val rmHello = s"rm ${homeVolume}hello.txt".!!
    info(copyToDockerVolume)
    info(chmodHello)
    info(createHDFSFile)
    info(chmodHDFSFile)
    info(createHDFSFile2)
    info(chmodHDFSFile2)
    info(rmHello)

    info("\n--------Test: Create tables -----------")
    spark.sql("create database if not exists testdatabase")
    spark.sql("create table if not exists testdatabase.tableInput1 (a String, b String) partitioned by (year Int, month Int, day Int)")
    spark.sql("create table if not exists testdatabase.tableInput2 (a String, b Int) partitioned by (year Int, month Int, day Int)")
    spark.sql("create table if not exists testdatabase.tableOutput (a String, b Int) partitioned by (year Int, month Int, day Int)")
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    info("\n--------Test: Insert in tables-----------")
    import spark.implicits._
    List(("Pre", "b", 2222, 33, 44)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.tableInput1")
    List(("Pre2", "b", 1234, 56, 78)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.tableInput2")
  }
}
