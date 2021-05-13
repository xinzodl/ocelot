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

package com.bbva.ebdm.ocelot.engines.spark

import scala.collection.immutable

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.{ConfigOps, Engine, SettingsOps, StringOps}

/** Create a SoarkEngine overriding types and values of [[com.bbva.ebdm.ocelot.core.Engine]]. */
object SparkEngine extends Engine with LogSupport {
  override val engineName = "spark"
  override type EnvType = SparkSession
  override type DataType = DataFrame

  /**
    *  Resolves a Config to a SparkSession.
    *
    * @param config A Configuration containing necessary keys.
    * @return The environment, a SparkSession
    */
  override def getEnv(config: Config): EnvType = {
    val appName = config.at("app.name".path[String]).getOrFail("app name not specified")
    val options = config.at("engines.spark".path[immutable.Map[String, String]]).getOrElse(immutable.Map.empty)

    info(s"Creating SparkSession with: appName=$appName options=$options and Hive support")

    // Builder with mandatory Hive dependencies
    var builder = SparkSession.builder.appName(appName).config("spark.sql.catalogImplementation", "hive")

    // Setting options
    for ((k, v) <- options) {
      builder = builder.config(k, v)
    }

    // Create Session and return it
    builder.getOrCreate()
  }
}
