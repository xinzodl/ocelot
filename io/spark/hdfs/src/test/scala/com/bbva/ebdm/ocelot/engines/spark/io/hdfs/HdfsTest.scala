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

package com.bbva.ebdm.ocelot.engines.spark.io.hdfs

import scala.sys.process.stringToProcess

import com.typesafe.config.{Config, ConfigException}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.ConfigLoader
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine
import com.bbva.ebdm.ocelot.templates.base.BaseApp

class HdfsTest extends FunSuite with Matchers with BeforeAndAfterAll with LogSupport {

  class MyBaseApp(cf: Config) extends {
    val engine = SparkEngine
  } with BaseApp {
    override def exec(): Unit = Unit
    override lazy val config: Config = cf.withFallback(ConfigLoader.load(Array()))
    override def process(inputMap: Map[String, DataFrame]): Map[String, DataFrame] = Map()
  }

  def obj(cf: Config) = new MyBaseApp(cf)

  val fullConfig: Config = ConfigLoader.load(Array()).getConfig("HdfsTest")
  lazy implicit val sse: SparkSession = SparkEngine.getEnv(ConfigLoader.load(Array()))

  // Input errors
  test("should throw error when path key is missing in Hdfs Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("pathKeyMissingIN")).inputs should have message
      "Missing path in Hdfs Input"
  }
  test("should throw error when path value is empty in Hdfs Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("pathValueEmptyIN")).inputs should have message
      "Path in Hdfs Input is empty"
  }
  test("should throw error when formatStr key is missing in Hdfs Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("formatStrKeyMissingIN")).inputs should have message
      "Missing formatStr in Hdfs Input"
  }
  test("should throw error when formatStr value is not valid in Hdfs Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("formatStrValueInvalidIN")).inputs should have message
      "Parameter formatStr=fakeFormat in HdfsInput is not a valid format"
  }

  // Output errors
  test("should throw error when subtype key is missing in Hdfs Output") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("subtypeKeyMissingOUT")).outputs should have message
      "Missing subtype in Hdfs Output"
  }
  test("should throw error when subtype key is not valid in Hdfs Output") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("subtypeKeyInvalidOUT")).outputs should have message
      "Invalid Subtype in Hdfs Output: fakeSubType"
  }
  test("should throw error when path key is missing in Hdfs Output") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("pathKeyMissingOUT")).outputs should have message
      "Missing path in Hdfs file Output"
  }
  test("should throw error when formatStr key is missing in Hdfs Row Output") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("formatStrKeyMissingOUT")).outputs should have message
      "Missing formatStr in Hdfs row Output"
  }
  test("should throw error when formatStr value is not valid in Hdfs Row Output") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("formatStrValueInvalidOUT")).outputs should have message
      "Parameter formatStr=fakeFormat in HdfsOutput Row is not a valid format"
  }

  // Input OK
  test("should read a text file from hdfs") {
    obj(fullConfig.getConfig("validTextIN")).inputs("1234").read().take(1).mkString shouldBe "[Esto es; una prueba;]"
  }

  // Outputs OK
  test("should delete an existing text file from hdfs") {
    import sse.implicits._
    sse.read.text("/tmp/deleteThis.txt").count() shouldBe 4
    obj(fullConfig.getConfig("validDeleteOut")).outputs("1234").write(Seq("").toDF)
    val exception = the [AnalysisException] thrownBy sse.read.text("/tmp/deleteThis.txt").count()
    exception.getMessage().contains("Path does not exist: hdfs://172.26.0.9:9000/tmp/deleteThis.txt")
  }
  test("should try to delete a non existing text file from hdfs and not throw an error") {
    import sse.implicits._
    val exception = the [AnalysisException] thrownBy sse.read.text("/tmp/deleteThis.txt").count()
    exception.getMessage().contains("Path does not exist: hdfs://172.26.0.9:9000/tmp/deleteThis.txt")
    noException should be thrownBy obj(fullConfig.getConfig("validDeleteOut")).outputs("1234").write(Seq("").toDF)
  }
  test("should write to a text file in hdfs (File subtype)") {
    val str = "Este es el string que se va a escribir"
    import sse.implicits._
    noException should be thrownBy obj(fullConfig.getConfig("validFileOut")).outputs("1234")
      .write(Seq(("name.txt",str)).toDF("name", "value"))
    sse.read.text("/tmp/file/name.txt").take(1)(0).getString(0) shouldBe str
  }
  test("should write to a text file in hdfs (String subtype)") {
    val str1 = "Este es el string que"
    val str2 = "se va a escribir"
    import sse.implicits._
    noException should be thrownBy obj(fullConfig.getConfig("validStringOut")).outputs("1234")
      .write(Seq(str1,str2).toDF)
    sse.read.text("/tmp/file/stringFile.txt").take(2).mkString(" ") shouldBe s"[$str1] [$str2]"
  }
  test("should write to a text file in hdfs (Row subtype) II") {
    val str1 = "Este es el string que"
    val str2 = "se va a escribir"
    import sse.implicits._
    noException should be thrownBy obj(fullConfig.getConfig("validRowPartitionOut")).outputs("1234")
      .write(Seq((str1, 2005),(str2, 2005),(str2, 2006)).toDF("name","year"))
    sse.read.text("/tmp/row2/year=2005").count shouldBe 2
    sse.read.text("/tmp/row2/year=2006").count shouldBe 1
  }


  // Get Docker ready
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

    sse.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://172.26.0.9:9000")

    info("\n--------Test: Create files in HDFS -----------")
    val HelloPath: String = getClass.getResource("/hello.txt").getPath
    info(s"cp $HelloPath $homeVolume".!!)
    info(s"chmod 777 ${homeVolume}hello.txt".!!)
    info(s"docker exec $dockerName hdfs dfs -copyFromLocal ${dockerVolume}hello.txt /tmp/hello.txt".!!)
    info(s"docker exec $dockerName hdfs dfs -chmod 777 /tmp/hello.txt".!!)
    info(s"docker exec $dockerName hdfs dfs -copyFromLocal ${dockerVolume}hello.txt /tmp/deleteThis.txt".!!)
    info(s"docker exec $dockerName hdfs dfs -chmod 777 /tmp/deleteThis.txt".!!)
    info(s"rm ${homeVolume}hello.txt".!!)

  }
}
