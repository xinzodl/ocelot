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

package com.bbva.ebdm.ocelot.templates.base

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import com.typesafe.config.{Config, ConfigException}
import wvlet.airframe.{Design, bind, newDesign}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.{ConfigLoader, ConfigOps, Engine, SettingsOps, StringOps}

/** Call read data, the Business Logic on the data and finally write the data. */
trait BaseApp extends LogSupport {
  lazy val config: Config = bind[Config]
  val engine: Engine
  implicit val env: engine.EnvType = engine.getEnv(config)

  /**
    *  The Business Logic.
    *
    * @param inputMap An id and a [[com.bbva.ebdm.ocelot.core.Engine.DataType]] in a Map: Map[id, [[com.bbva.ebdm.ocelot.core.Engine.DataType]]].
    * @return An id and a [[com.bbva.ebdm.ocelot.core.Engine.DataType]] in a Map: Map[id, [[com.bbva.ebdm.ocelot.core.Engine.DataType]]].
    */
  def process(inputMap: Map[String, engine.DataType]): Map[String, engine.DataType]

  /**
    *  Call [[com.bbva.ebdm.ocelot.core.Engine.Input]] read, the Business Logic [[com.bbva.ebdm.ocelot.templates.base.BaseApp]] process and finally [[com.bbva.ebdm.ocelot.core.Engine.Output]] write.
    *
    * @return Nothing.
    */
  def exec(): Unit

  /**
    *  Resolves "val config: Config" into a Map with a String id and a [[com.bbva.ebdm.ocelot.core.Engine.Input]]: Map[id, [[com.bbva.ebdm.ocelot.core.Engine.Input]]].
    */
  val inputs: Map[String, engine.Input] = createInputOutput[engine.Input](inChoice)

  /**
    *  Resolves "val config: Config" into a Map with a String id and a [[com.bbva.ebdm.ocelot.core.Engine.Output]]: Map[id, [[com.bbva.ebdm.ocelot.core.Engine.Output]]].
    */
  val outputs: Map[String, engine.Output] = createInputOutput[engine.Output](outChoice)

  // In or Out representation
  private sealed abstract class InOrOut(val name: String) {
    override def toString: String = name
  }
  private object outChoice extends InOrOut("Output")
  private object inChoice extends InOrOut("Input")

  /**
    *  Resolves "val config: Config" into a Map with an id and a [[com.bbva.ebdm.ocelot.core.Engine.IO]] subClass.
    *
    * @param inOrOut String with the word "Input" or "Output".
    * @return An id and a [[com.bbva.ebdm.ocelot.core.Engine.IO]] subClass in a Map: Map[id, [[com.bbva.ebdm.ocelot.core.Engine.IO subClass]]].
    */
  private def createInputOutput[T <: engine.IO: ClassTag](inOrOut: InOrOut): Map[String, T] = {
    val inOrOutLower: String = inOrOut.name.toLowerCase
    debug(s"Creating $inOrOut")

    val dataMap = config.at(s"${inOrOutLower}s".path[Seq[Config]]).getOrFail(s"${inOrOut}s key not specified") match {
      case sq if sq.isEmpty =>
        throw new ConfigException.Generic(s"${inOrOut}s key is an empty sequence, at least one $inOrOutLower is needed")
      case sq =>
        sq.foldLeft(Map[String, T]()) { (acc, k) =>
          val kType = k.at("type".path[String]).getOrFail(s"$inOrOut type not specified").toLowerCase
          val firstLetterUpperCase: String = kType.replaceFirst(kType.substring(0, 1), kType.substring(0, 1).toUpperCase)
          val className = s"com.bbva.ebdm.ocelot.engines.${engine.engineName}.io.$kType.$firstLetterUpperCase" + "IO"
          val id = k.at("id".path[String]).getOrFail(s"Missing id in $kType $inOrOut")
          val methodName = s"create$inOrOut"

          debug(s"Invoking method=$methodName from class=$className with config=$k")
          Try {
            val loadClass = this.getClass.getClassLoader.loadClass(className)
            loadClass.getDeclaredMethod(methodName, classOf[Config]).invoke(loadClass, k)
          } match {
            case Success(v: T) => acc + (id -> v)
            case Success(_) =>
              throw new ConfigException.Generic(s"Invalid Symbol in ${inOrOut}s returned by: $className")
            case Failure(_: ClassNotFoundException) =>
              throw new ConfigException.Generic(s"Invalid Type in $inOrOut: $kType")
            case Failure(f) =>
              throw f.getCause
          }
        }
    }

    debug(s"Created ${inOrOut}s for this ids: ${dataMap.keys.mkString(",")}")
    dataMap
  }
}


/** Factory for [[com.bbva.ebdm.ocelot.templates.base.BaseApp]] Design. */
object BaseApp extends LogSupport {
  /**
    *  Getting wvlet(airframe) newDesign, binding Configuration and String array.
    *
    * @param args String array with the configuration parameters.
    * @return A new Design.
    */
  def design(args: Array[String]): Design = {
    debug(s"Getting wvlet(airframe) newDesign, binding args and config")
    newDesign
      .noLifeCycleLogging
      .bind[Array[String]].toInstance(args)
      .bind[Config].toProvider { args: Array[String] => ConfigLoader.load(args) }
  }
}
