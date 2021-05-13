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

package com.bbva.ebdm.ocelot.engines.spark.io.streaming

import scala.sys.process.stringToProcess

import com.typesafe.config.{Config, ConfigException}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.ConfigLoader
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine
import com.bbva.ebdm.ocelot.templates.base.BaseApp

class StreamingTest extends FunSuite with Matchers with BeforeAndAfterAll with LogSupport {

  class MyBaseApp(cf: Config) extends {
    val engine = SparkEngine
  } with BaseApp {
    override def exec(): Unit = Unit
    override lazy val config: Config = cf.withFallback(ConfigLoader.load(Array()))
    override def process(inputMap: Map[String, DataFrame]): Map[String, DataFrame] = Map()
  }

  def obj(cf: Config) = new MyBaseApp(cf)

  val fullConfig: Config = ConfigLoader.load(Array()).getConfig("StreamingTest")
  lazy implicit val sse: SparkSession = SparkEngine.getEnv(ConfigLoader.load(Array()))


  // ------------------------------- Test input errors -------------------------------
  test("should throw error with Input streaming not created Format") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("streamingnotINFormat")).inputs should have message
      "Missing format in Streaming Input"
  }

  test("should load complete streamingInput") {
    val in: Map[String, SparkEngine.Input] = obj(fullConfig.getConfig("streamingIN")).inputs
    val error = the [AnalysisException] thrownBy in("streamingExample").read().collect()
    error.getMessage shouldBe "Queries with streaming sources must be executed with writeStream.start();;\nFileSource[/tmp/]"
  }

  test("should load complete streamingInput simple") {
    val in: Map[String, SparkEngine.Input] = obj(fullConfig.getConfig("streamingINSimple")).inputs
    val error = the [IllegalArgumentException] thrownBy in("streamingExample").read().collect()
    error.getMessage shouldBe "'path' is not specified"
  }

  // ------------------------------- Test output errors -------------------------------
  test("should throw error with Output streaming not created Format") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("streamingnotOUTFormat")) should have message
      "Missing format in Streaming Output"
  }

  test("should load a complete streamingOutput simple") {
    val in: Map[String, SparkEngine.Output] = obj(fullConfig.getConfig("streamingOutSimple")).outputs
    val error = the [IllegalArgumentException] thrownBy in("streamingExample")
      .write(obj(fullConfig.getConfig("streamingIN")).inputs("streamingExample").read())
    error.getMessage shouldBe "'path' is not specified"
  }

  test("should load a complete streamingOutput simple trigger Once") {
    val in: Map[String, SparkEngine.Output] = obj(fullConfig.getConfig("streamingOutSimpleTriggerOnce")).outputs
    val error = the [IllegalArgumentException] thrownBy in("streamingExample")
      .write(obj(fullConfig.getConfig("streamingIN")).inputs("streamingExample").read())
    error.getMessage shouldBe "'path' is not specified"
  }

  test("should load a complete streamingOutput trigger Continuous") {
    val in: Map[String, SparkEngine.Output] = obj(fullConfig.getConfig("streamingOutTriggerContinuous")).outputs
    val error = the [IllegalArgumentException] thrownBy in("streamingExample")
      .write(obj(fullConfig.getConfig("streamingIN")).inputs("streamingExample").read())
    error.getMessage shouldBe "'path' is not specified"
  }

  // ------------------------------- Test output success -------------------------------
  test("should load a complete streamingOutput trigger ProcessingTime") {
    val in: Map[String, SparkEngine.Output] = obj(fullConfig.getConfig("streamingOut")).outputs
    in("streamingExample")
      .write(obj(fullConfig.getConfig("streamingIN")).inputs("streamingExample").read())
    s"docker exec $dockerName hdfs dfs -ls /tmp/world/".!!.contains("/tmp/world/_spark_metadata") shouldBe true
  }

  val dockerName: String = "docker_spark_streaming"
  val dockerName_net: String = s"${dockerName}_net"
  val ip: String = "172.26.0.9"
  val subnet: String = ip.substring(0, ip.lastIndexOf(".") + 1) + "0/16"
  val masterPort: String = "7077"
  val slavePort: String = "35261"
  val hdfsPort: String = "9000"
  val kafkaPort: String = "9092"
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
      s"-d --name $dockerName $dockerImage").!!
    info(runDocker)

    info("\n--------Docker run script startup-----------")
    val runStartup: String = s"docker exec $dockerName /opt/script/startup.sh".!!
    info(runStartup)

    sse.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://172.26.0.9:9000")
    val createHDFSFile: String = s"docker exec $dockerName hdfs dfs -copyFromLocal /opt/script/startup.sh /tmp/hello.txt".!!
    info(createHDFSFile)
  }
}
