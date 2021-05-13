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

package com.bbva.ebdm.ocelot.engines.spark.io.hive

import scala.sys.process.stringToProcess

import com.typesafe.config.{Config, ConfigException}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.AccessControlException
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.ConfigLoader
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine
import com.bbva.ebdm.ocelot.templates.base.BaseApp

class HiveTest extends FunSuite with Matchers with BeforeAndAfterAll with LogSupport {

  class MyBaseApp(cf: Config) extends {
    val engine = SparkEngine
  } with BaseApp {
    override def exec(): Unit = Unit
    override lazy val config: Config = cf.withFallback(ConfigLoader.load(Array()))
    override def process(inputMap: Map[String, DataFrame]): Map[String, DataFrame] = Map()
  }

  def obj(cf: Config) = new MyBaseApp(cf)
  
  val fullConfig: Config = ConfigLoader.load(Array()).getConfig("HiveTest")
  lazy implicit val sse: SparkSession = SparkEngine.getEnv(ConfigLoader.load(Array()))

  // ------------------------------- Test input errors -------------------------------
  test("should throw error with missing schema in hive Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("missingSchemaINhive")).inputs should have message
      "Missing schema in Hive Input"
  }
  test("should throw error with missing table in hive Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("missingTableINhive")).inputs should have message
      "Missing table in Hive Input"
  }
  test("should throw error with invalid date (length 2) in Hive Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("invalidDateINhive2")).inputs should have message
      "Invalid Date: 20"
  }

  test("should read a simple HiveInput with no date") {
    val in: Map[String, SparkEngine.Input]  = obj(fullConfig.getConfig("simpleINhive")).inputs
    val error = the [AccessControlException] thrownBy in("1234").read().collect()
    error.getMessage.contains("Permission denied: ") shouldBe true
  }

  // ------------------------------- Test output errors -------------------------------
  test("should throw error with schema not specified in Hive output") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("missingSchemaOUThive")).outputs should have message
      "Missing schema in Hive Output"
  }

  test("should throw error with table not specified in Hive output") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("missingTableOUThive")).outputs should have message
      "Missing table in Hive Output"
  }

  test("should not read a simple HiveOutput") {
    import sse.implicits._
    val in: Map[String, SparkEngine.Output] = obj(fullConfig.getConfig("simpleOUThive")).outputs
    val error = the [AnalysisException] thrownBy in("1234")
      .write(List((1234,56)).toDF("year", "month"))
    error.getMessage() shouldBe "Table or view not found: mySchema.myTable;"
  }

  test("should not read HiveOutput with override Hdfs Partitions") {
    import sse.implicits._
    val in: Map[String, SparkEngine.Output] = obj(fullConfig.getConfig("overrideHdfsPartitionsOUThive")).outputs
    val error = the [AnalysisException] thrownBy in("1234")
      .write(List((1234,56)).toDF("year", "month"))
    error.getMessage() shouldBe "Table or view not found: mySchema.myTable;"
  }

  test("should read a simple HiveInput with date = year") {
    val in: Map[String, SparkEngine.Input] = obj(fullConfig.getConfig("completeINhiveYear")).inputs
    val error = the [AccessControlException] thrownBy in("1234").read().collect()
    error.getMessage.contains("Permission denied: ") shouldBe true
  }

  test("should not read a simple HiveOutput with override, path to HDFS doen't exist") {
    import sse.implicits._
    sse.sparkContext.hadoopConfiguration.set("fs.defaultFS", "file:///")
    val in: Map[String, SparkEngine.Output] = obj(fullConfig.getConfig("overrideOUThive")).outputs
    val error = the [IllegalArgumentException] thrownBy in("1234")
      .write(List(("a", "b", 1234, 56, 78), ("a", "b", 1234, 56, 77), ("a", "b", 1234, 56, 77), ("a", "b", 1234, 55, 77))
        .toDF("name", "value", "year", "month", "day"))
    error.getMessage.contains("Wrong FS: hdfs://172.26.0.9:9000/user/hive/warehouse/testdatabase.db") shouldBe true
  }

  // ------------------------------- Test success - generate inputs -------------------------------

  test("should read a simple HiveInput with date = year+month+day") {
    val in: Map[String, SparkEngine.Input]  = obj(fullConfig.getConfig("completeINhiveDate")).inputs
    in("1234").read().collect() shouldBe Array()
  }

  test("should read a simple HiveInput with date = year+month") {
    val in: Map[String, SparkEngine.Input]  = obj(fullConfig.getConfig("completeINhiveMonth")).inputs
    in("1234").read().collect() shouldBe Array()
  }

  // ------------------------------- Test success - generate outputs -------------------------------

  test("should read a simple HiveOutput with override") {
    import sse.implicits._
    sse.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://172.26.0.9:9000")
    val in: Map[String, SparkEngine.Output]  = obj(fullConfig.getConfig("overrideOUThive")).outputs
    in("1234")
      .write(List(("a", "b", 1234, 56, 78), ("a", "b", 1234, 56, 77), ("a", "b", 1234, 56, 77), ("a", "b", 1234, 55, 77))
        .toDF("name", "value", "year", "month", "day"))

    sse.sql("select * from testdatabase.overrideHdfsPartitionsDiferentPartition where year == 1234").collect() should
      contain(Row("a", null, 1234, 55, 77))
  }

  test("getMaxPartition") {
    getMaxPartition(sse, "testdatabase", "maxPartition") shouldBe Array(3333,44,55)
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

    info("\n--------Test: Create tables -----------")
    sse.sql("create database if not exists testdatabase")
    sse.sql("create table if not exists testdatabase.overrideHdfsPartitionsDiferentPartition (a String, b Int) partitioned by (year Int, month Int, day Int)")
    sse.sql("create table if not exists testdatabase.maxPartition (a String, b Int) partitioned by (year Int, month Int, day Int)")
    sse.conf.set("hive.exec.dynamic.partition", "true")
    sse.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    info("\n--------Test: Insert in tables-----------")
    import sse.implicits._
    List(("Pre", "b", 1111, 33, 44)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.overrideHdfsPartitionsDiferentPartition")
    List(("Pre2", "b", 1234, 55, 77)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.overrideHdfsPartitionsDiferentPartition")
    List(("Pre4", "b", 1234, 56, 77)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.overrideHdfsPartitionsDiferentPartition")
    List(("Pre2", "b", 1234, 56, 78)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.overrideHdfsPartitionsDiferentPartition")
    List(("Pre2", "b", 1234, 56, 78)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.overrideHdfsPartitionsDiferentPartition")
    List(("Pre", "b", 2222, 33, 44)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.overrideHdfsPartitionsDiferentPartition")
    List(("Pre3", "b", 3333, 44, 55)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.overrideHdfsPartitionsDiferentPartition")
    List(("Pre2", "b", 1234, 55, 77)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.maxPartition")
    List(("Pre3", "b", 3333, 44, 55)).toDF("name", "value", "year", "month", "day").write.mode(SaveMode.Append).insertInto("testdatabase.maxPartition")

    val chmodPartition: String = s"docker exec $dockerName hdfs dfs -chmod 000 /user/hive/warehouse/testdatabase.db/overridehdfspartitionsdiferentpartition/year=2222/*/*/".!!
    val chmodPartition1: String = s"docker exec $dockerName hdfs dfs -chmod -R 555 /user/hive/warehouse/testdatabase.db/overridehdfspartitionsdiferentpartition/year=1111/".!!
    val chmodPartition2: String = s"docker exec $dockerName hdfs dfs -chmod 000 /user/hive/warehouse/testdatabase.db/overridehdfspartitionsdiferentpartition/year=3333/*/*/*".!!
    info(chmodPartition)
    info(chmodPartition1)
    info(chmodPartition2)
  }
}
