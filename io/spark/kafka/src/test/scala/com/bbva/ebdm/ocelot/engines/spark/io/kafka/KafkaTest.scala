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

package com.bbva.ebdm.ocelot.engines.spark.io.kafka

import scala.sys.process.stringToProcess
import scala.util.Try

import com.typesafe.config.{Config, ConfigException}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.ConfigLoader
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine
import com.bbva.ebdm.ocelot.templates.base.BaseApp

class KafkaTest extends FunSuite with Matchers with BeforeAndAfterAll with LogSupport {

  class MyBaseApp(cf: Config) extends {
    val engine = SparkEngine
  } with BaseApp {
    override def exec(): Unit = Unit
    override lazy val config: Config = cf.withFallback(ConfigLoader.load(Array()))
    override def process(inputMap: Map[String, DataFrame]): Map[String, DataFrame] = Map()
  }

  def obj(cf: Config) = new MyBaseApp(cf)
  
  val fullConfig: Config = ConfigLoader.load(Array()).getConfig("KafkaTest")
  lazy implicit val sse: SparkSession = SparkEngine.getEnv(ConfigLoader.load(Array()))

  // ------------------------------- Test input errors -------------------------------
  test("should throw error with missing bootstrapServers in kafka Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("missingBootstrapServersINkafka")).inputs should have message
      "Missing bootstrapServers in Kafka Input"
  }
  test("should throw error with missing topics in kafka Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("missingTopicsINkafka")).inputs should have message
      "Missing topics in Kafka Input"
  }

  // ------------------------------- Test output errors -------------------------------
  test("should throw error with bootstrapServers not specified in Kafka output") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("missingBootstrapServersOUTkafka")).outputs should have message
      "Missing bootstrapServers in Kafka Output"
  }

  // ------------------------------- Test success - generate inputs -------------------------------
  test("should read and write to kafka in batch mode with completeINkafka") {
    import sse.implicits._
    // write data to kafka
    val df1 = List(("tp1","holaaa, que tal todo?")).toDF("topic","value")
    df1.write
      .format("kafka").option("kafka.bootstrap.servers", "172.26.0.9:9092").save()

    // write to kafka with KafkaIO
    val df2 = List(("tp1","pues por aqui, todo bien")).toDF("topic","value")
    obj(fullConfig.getConfig("completeINkafka")).outputs("1234").write(df2)

    // read with KafkaIO
    val dfResult = obj(fullConfig.getConfig("completeINkafka")).inputs("1234").read()

    dfResult.select("value").collect() shouldBe df1.union(df2).select("value").collect()
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
  val userHome: String =  System.getProperty("user.home")
  val homeVolume: String = userHome + "/Documentos/ocelotVolume/"
  val dockerVolume: String = "/opt/volume/"

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

    info("\n--------Docker start kafka-----------")
    // Order with kafka scripts matters
    val fileNames = List("docker_zookeeper_start.sh", "docker_kafka_start.sh", "docker_topic_start.sh")
    val localFilePaths = fileNames.map( x => getClass.getResource(s"/$x").getPath )
    fileNames.map( x => s"rm -f $homeVolume$x".!! )
    localFilePaths.map( x => s"cp $x $homeVolume".!! )
    fileNames.map( x => s"chmod 777 $homeVolume$x".!! )
    fileNames.map( x => s"docker exec -d $dockerName $dockerVolume$x".!! )
    info("Kafka started, topic created")
  }
}
