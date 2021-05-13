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

import com.typesafe.config.Config
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.{ConfigOps, SettingsOps, StringOps}
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine.{DataType, EnvType, Input, Output}

/**
  *  Create [[com.bbva.ebdm.ocelot.core.Engine.Input]] Subclass.
  *
  * @param bootstrapServers Bootstrap Servers like "IP:PORT"
  * @param topics Topics to be read
  * @param topicIsPattern If topic specifies topic pattern rather than topic names
  * @param getOnlyTimestampAndValue Select only timestamp and value fields from kafka
  * @param optMap Map with options
  * @param selectExpr Custom select expression
  */
case class KafkaInput(
  bootstrapServers: Seq[String],
  topics: Seq[String],
  topicIsPattern: Boolean = false,
  getOnlyTimestampAndValue: Boolean = false,
  optMap: Option[Map[String, String]] = None,
  selectExpr: Option[Seq[String]]
) extends Input with LogSupport {

  /**
    *  Read from Kafka and generate a [[DataType]].
    *
    * @param env An implicit [[EnvType]]
    * @return [[DataType]].
    */
  override def read()(implicit env: EnvType): DataType = {
    debug(s"Read DataFrame from kafka with: bootstrapServers:$bootstrapServers topics=$topics " +
      s"topicIsPattern=$topicIsPattern getOnlyTimestampAndValue=$getOnlyTimestampAndValue " +
      s"optMap=$optMap selectExpr=$selectExpr")

    val bootstrapServersList = bootstrapServers.mkString(", ")
    val topicsList = topics.mkString(", ")
    val opts: Map[String, String] = optMap.getOrElse(Map())
    val optsWithServer = opts + ("kafka.bootstrap.servers"-> bootstrapServersList)
    val optsWithTopic = optsWithServer + ((if (topicIsPattern) "subscribePattern" else "subscribe") -> topicsList)

    val df = env.read.format("kafka").options(optsWithTopic).load()
    val select = getOnlyTimestampAndValue match {
      case true => List("CAST(timestamp AS TIMESTAMP)", "CAST(value AS STRING)")
      case false => selectExpr.getOrElse(List("*"))
    }

    df.selectExpr(select:_*)
  }
}

/**
  *  Create [[com.bbva.ebdm.ocelot.core.Engine.Input]] Subclass.
  *
  * @param bootstrapServers Bootstrap Servers like "IP:PORT"
  * @param topic Topic name, if not specified, column topic is necessary in dataframe
  * @param optMap Map with options
  */
case class KafkaOutput(
  bootstrapServers: Seq[String],
  topic: Option[String] = None,
  optMap: Option[Map[String, String]] = None
) extends Output with LogSupport {

  /**
    *  Write to Kafka a [[DataType]].
    *
    * @param data A [[DataType]]
    * @param env An implicit [[EnvType]]
    * @return Nothing.
    */
  override def write(data: DataType)(implicit env: EnvType): Unit = {
    debug(s"Read DataFrame from kafka with: bootstrapServers:$bootstrapServers topic=$topic optMap=$optMap")

    val bootstrapServersList = Map("kafka.bootstrap.servers" -> bootstrapServers.mkString(", "))
    val topicOpt = if (topic.isDefined) Map("topic" -> topic.get) else Map()
    val opts = optMap.getOrElse(Map()) ++ topicOpt ++ bootstrapServersList

    data.write.format("kafka").options(opts).save
  }

}

/** Factory to create [[com.bbva.ebdm.ocelot.core.Engine.IO]] Subclasses. */
object KafkaIO extends LogSupport {

  /**
    *  Resolve Configuration to [[com.bbva.ebdm.ocelot.core.Engine.Input]] Subclass.
    *
    * @param k A Configuration.
    * @return [[KafkaInput]].
    */
  final def createInput(k: Config): KafkaInput = {
    val bootstrapServers = k.at("bootstrapServers".path[Seq[String]]).getOrFail("Missing bootstrapServers in Kafka Input")
    val topics = k.at("topics".path[Seq[String]]).getOrFail("Missing topics in Kafka Input")
    val topicIsPattern = k.at("topicIsPattern".path[Boolean]).getOrElse(false)
    val getOnlyTimestampAndValue = k.at("getOnlyTimestampAndValue".path[Boolean]).getOrElse(false)
    val optMap = k.at("optMap".path[Map[String, String]]).toOption
    val selectExpr = k.at("selectExpr".path[Seq[String]]).toOption

    debug(s"Creating KafkaInput with: bootstrapServers=$bootstrapServers topics=$topics topicIsPattern=$topicIsPattern " +
      s"getOnlyTimestampAndValue=$getOnlyTimestampAndValue optMap=$optMap selectExpr=$selectExpr")
    KafkaInput(bootstrapServers, topics, topicIsPattern, getOnlyTimestampAndValue, optMap, selectExpr)
  }

  /**
    *  Resolve Configuration to [[com.bbva.ebdm.ocelot.core.Engine.Output]] Subclass.
    *
    * @param k A Configuration.
    * @return [[KafkaOutput]].
    */
  final def createOutput(k: Config): KafkaOutput = {
    val bootstrapServers = k.at("bootstrapServers".path[Seq[String]]).getOrFail("Missing bootstrapServers in Kafka Output")
    val topics = k.at("topics".path[String]).toOption
    val optMap = k.at("optMap".path[Map[String, String]]).toOption

    debug(s"Creating kafkaOutput with: bootstrapServers=$bootstrapServers topics=$topics optMap=$optMap")
    KafkaOutput(bootstrapServers, topics, optMap)
  }

}
