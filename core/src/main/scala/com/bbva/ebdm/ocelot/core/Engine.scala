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

package com.bbva.ebdm.ocelot.core

import com.typesafe.config.Config

/**
  *  The project's Engine. The types and values members must be overridden for any specific Engine.
  *
  * {{{
  * SparkEngine Example:
  *   override val engineName = "spark"
  *   override type EnvType = SparkSession
  *   override type DataType = DataFrame
  *   override def read()(implicit env: EnvType): DataType = {
  *     spark.read.format("text").load(path)
  *   }
  *   override def write(data: DataType)(implicit env: EnvType): Unit = {
  *     env.write.format("text").save(path)
  *   }
  *   override def getEnv(config: Config): SparkSession = {
  *     SparkSession.builder.appName(config.at("app.name"
  *       .path[String]).getOrFail("app name not specified"))
  *       .getOrCreate()
  *   }
  *   }}}
  */
trait Engine {

  /**
    * The type of the Engine's Environment.
    *
    * {{{
    *  For instance, if we are using Spark, this EnvType is "SparkSesion"
    * }}}
    */
  type EnvType

  /**
    * The type of data read and written.
    * {{{
    * For instance, if we are using Spark, this DataType could be "DataFrame" or "Dataset[_]"
    * }}}
    */
  type DataType

  sealed trait IO

  /**
    * The Input data.
    */
  trait Input extends IO {

    /**
      *  Read data.
      *
      * @param env A environment.
      * @return Read data.
      */
    def read()(implicit env: EnvType): DataType
  }

  /**
    * The Output data.
    */
  trait Output extends IO {

    /**
      *  Write data.
      *
      * @param data The data to write.
      * @param env A environment.
      * @return Nothing.
      */
    def write(data: DataType)(implicit env: EnvType): Unit
  }

  /**
    * The Engine's name.
    *
    * Example, if we are using Spark, this engineName is "spark".
    */
  def engineName: String

  /**
    *  Creates an EnvType from a given configuration
    *
    * @param config A Configuration containing all necessary setting to create the environment.
    * @return A environment.
    */
  def getEnv(config: Config): EnvType
}
