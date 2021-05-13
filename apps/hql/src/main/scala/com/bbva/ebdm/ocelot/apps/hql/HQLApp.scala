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

package com.bbva.ebdm.ocelot.apps.hql

import scala.io.Source

import com.typesafe.config.{Config, ConfigException}
import wvlet.airframe.{bind, Design}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.{ConfigOps, SettingsOps, StringOps}
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine
import com.bbva.ebdm.ocelot.templates.base.BaseApp

trait HQLApp extends LogSupport {
  lazy val config: Config = bind[Config]
  lazy val env: SparkEngine.EnvType = SparkEngine.getEnv(config)
  case class QuerySpec(typ: String, value: String)

  /**
    *  Read data and the Business Logic.
    *
    * @param fileName A String with a path to a file with all input queries.
    * @return A List of queries to write.
    */
  private def getHQLFomFile(fileName: String): Seq[Option[String]] = {
    val allQueries = Source
      .fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName))
      .getLines()
      .mkString(" ")

    val splittedQueries: Array[String] = allQueries.split(";").map(_.trim)
    splittedQueries.map(x => Some(x.resolveString(config)))
  }

  val queries: Seq[QuerySpec] = config.at("queries".path[Seq[Map[String, String]]])
    .getOrFail("Could not read queries").map(x => QuerySpec(x("type"), x("content")))

  val queriesSeq: Seq[String] = queries.foldLeft(Seq[Option[String]]()) { (acc, qs) =>
    val toAdd = qs match {
      case QuerySpec("string", query) => Seq(Some(query))
      case QuerySpec("file", filename) => getHQLFomFile(filename)
      case QuerySpec(unknown, _) => throw new ConfigException.Generic(s"Expected type 'string' or 'file' but found '$unknown'")
    }
    acc ++ toAdd
  }.flatten

  /**
    *  Run queries contained in [[queriesSeq]] using [[env]].sql(...)
    *
    * @return Nothing.
    */
  def exec(): Unit = {
    for (query <- queriesSeq) {
      env.sql(query)
      info(s"Executed query: $query")
    }
  }
}

/** Factory for [[com.bbva.ebdm.ocelot.apps.hql.HQLApp]] Design. */
object HQLApp {
  /**
    *  Create a Design calling [[com.bbva.ebdm.ocelot.templates.base.BaseApp]] Design.
    *
    * @param args String array with the configuration parameters.
    * @return A Design.
    */
  def design(args: Array[String]): Design = BaseApp.design(args)
}

/** The Main object to call. */
object Main {
  /**
    *  The main project function.
    *
    * @param args String array with the configuration parameters.
    * @return Nothing.
    */
  def main(args: Array[String]): Unit = {
    HQLApp.design(args).build[HQLApp] { app: HQLApp => app.exec() }
  }
}
