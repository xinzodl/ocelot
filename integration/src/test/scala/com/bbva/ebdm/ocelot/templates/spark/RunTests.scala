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

package com.bbva.ebdm.ocelot.templates.spark

import scala.sys.process.stringToProcess

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.ConfigLoader
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine

class RunTests extends FunSpec with LogSupport with Matchers with BeforeAndAfterAll {

  implicit lazy val spark: SparkSession = SparkEngine.getEnv(ConfigLoader.load(Array()))

  describe("integration test"){
    it("should run without errors"){

      val hiveWordCount: DataFrame = spark.table("word.over300")
      hiveWordCount.count shouldBe 0

      // Run program
      SparkExampleAppMain.main(Array())

      // Check results
      val top2WordsRow = hiveWordCount.take(2)
      val topWord = (top2WordsRow(0).getString(0), top2WordsRow(0).getInt(1))
      val topSecondWord = (top2WordsRow(1).getString(0), top2WordsRow(1).getInt(1))

      hiveWordCount.count shouldBe 6

      topWord._1 shouldBe "fakeWord"
      topWord._2 shouldBe 2000

      topSecondWord._1 shouldBe "de"
      topSecondWord._2 shouldBe 682
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
    info(s"docker exec $dockerName hdfs dfs -mkdir /tmp/file/".!!)
    info(s"docker exec $dockerName hdfs dfs -put ${dockerVolume}hello.txt /tmp/file/elQuijote.txt".!!)
    info(s"docker exec $dockerName hdfs dfs -chmod 777 /tmp/file/elQuijote.txt".!!)
    info(s"rm ${homeVolume}hello.txt".!!)

    info("\n--------Test: Create tables and isnert -----------")
    val sse = SparkEngine.getEnv(ConfigLoader.load(Array()))
    sse.sql("create database word")
    sse.sql("create table word.previouscount (value STRING, count INT)")
    sse.sql("create table word.over300 (value STRING, count INT)")
    sse.sql("insert into word.previouscount values('fakeWord', 2000)")
    sse.sql("insert into word.previouscount values('someOtherWord', 200)")
    sse.sql("insert into word.previouscount values('yetAnotherWord', 20)")
    sse.sql("insert into word.previouscount values('theLastWord', 2)")
  }
}
