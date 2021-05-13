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

import com.typesafe.config.Config
import org.apache.spark.sql.streaming.Trigger
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.{ConfigOps, SettingsOps, StringOps}
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine.{DataType, EnvType, Input, Output}

/**
  *  Create [[com.bbva.ebdm.ocelot.core.Engine.Input]] Subclass.
  *
  * @param formatStr String with format.
  * @param optMap Map with configuration options.
  * @param schemaStr Some String with schema.
  */
case class StreamingInput(
    formatStr: String,
    optMap: Map[String,String] = Map(""->""),
    schemaStr: Option[String] = None
  ) extends Input with LogSupport {

    /**
      *  Read in Streaming and generate a [[DataType]].
      *
      * @param env An implicit [[EnvType]]
      * @return [[DataType]].
      */
    override def read()(implicit env: EnvType): DataType = {
      val df = env
        .readStream
        .format(formatStr)
        .options(optMap)

      val dfSchema = schemaStr match {
        case Some(x) => df.schema(x)
        case None => df
      }

      debug(s"Reading streaming DataFrame with: formatStr=$formatStr optMap=$optMap schemaStr=$schemaStr")
      dfSchema.load()
    }
  }

  /**
    *  Create [[com.bbva.ebdm.ocelot.core.Engine.Output]] Subclass.
    *
    * @param formatStr String with format.
    * @param optMap Map with configuration options.
    * @param trigger Some String with time per trigger.
    * @param outputModeStr Some String with output mode.
    * @param partitionByStr Some List of String with column partitions.
    */
  case class StreamingOutput(
    formatStr: String,
    optMap: Map[String,String] = Map(""->""),
    trigger: Option[String] = None,
    outputModeStr: Option[String] = None,
    partitionByStr: Option[Seq[String]] = None
  ) extends Output with LogSupport {
    /**
      *  Write in Streaming a [[DataType]].
      *
      * @param data A [[DataType]]
      * @param env An implicit [[EnvType]]
      * @return Nothing.
      */
    override def write(data: DataType)(implicit env: EnvType): Unit = {
      val df = data
        .writeStream
        .format(formatStr)
        .options(optMap)

      val dfTrigger =
        trigger match {
          case Some("Once") => df.trigger(Trigger.Once())
          case Some(x: String) if x.toLowerCase.contains("continuous") => df.trigger(Trigger.Continuous(x.toLowerCase().replaceAll("continuous", "")))
          case Some(x) => df.trigger(Trigger.ProcessingTime(x))
          case None => df
        }

      val dfPartitionBy = partitionByStr match {
        case Some(x) => dfTrigger.partitionBy(x: _*)
        case None => dfTrigger
      }

      val dfOutputMode = outputModeStr match {
        case Some(x) => dfPartitionBy.outputMode(x)
        case None => dfPartitionBy
      }

      debug(s"Writing DataFrame to stream: formatStr=$formatStr optMap=$optMap trigger=$trigger " +
        s"outputModeStr=$outputModeStr partitionByStr=$partitionByStr")
      dfOutputMode.start()
    }
  }

/** Factory to create [[com.bbva.ebdm.ocelot.core.Engine.IO]] Subclasses. */
object StreamingIO extends LogSupport {

  /**
    *  Resolve Configuration to [[com.bbva.ebdm.ocelot.core.Engine.Input]] Subclass.
    *
    * @param k A Configuration.
    * @return [[com.bbva.ebdm.ocelot.engines.spark.io.streaming.StreamingInput]].
    */
  final def createInput(k: Config): StreamingInput = {
    val formatStr = k.at("formatStr".path[String]).getOrFail("Missing format in Streaming Input")
    val optMap = k.at("optMap".path[Map[String,String]]).getOrElse(Map("" -> ""))
    val schemaStr = k.at("schemaStr".path[String]).toOption

    debug(s"Creating StreamingInput with: formatStr=$formatStr optMap=$optMap schemaStr=$schemaStr")
    StreamingInput(formatStr, optMap, schemaStr)
  }

  /**
    *  Resolve Configuration to [[com.bbva.ebdm.ocelot.core.Engine.Output]] Subclass.
    *
    * @param k A Configuration.
    * @return [[com.bbva.ebdm.ocelot.engines.spark.io.streaming.StreamingOutput]].
    */
  final def createOutput(k: Config): StreamingOutput = {
    val formatStr = k.at("formatStr".path[String]).getOrFail("Missing format in Streaming Output").toLowerCase
    val optMap = k.at("optMap".path[Map[String,String]]).getOrElse(Map("" -> ""))
    val trigger = k.at("trigger".path[String]).toOption
    val outputModeStr = k.at("outputModeStr".path[String]).toOption
    val partitionByStr = k.at("partitionByStr".path[Seq[String]]).toOption

    debug(s"Creating StreamingOutput with: formatStr=$formatStr optMap=$optMap trigger=$trigger " +
      s"outputModeStr=$outputModeStr partitionByStr=$partitionByStr")
    StreamingOutput(formatStr, optMap, trigger, outputModeStr, partitionByStr)
  }
}
