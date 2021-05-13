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

package com.bbva.ebdm.ocelot.archetypes.spark

import com.bbva.ebdm.ocelot.templates.spark.SparkBaseApp

/** Class which reads data, runs the Business Logic on the data and finally writes the data using Spark engine. */
class SparkArchetype extends SparkBaseApp {
  lazy val spark: engine.EnvType = env
  import spark.implicits._

  /**
    *  The Business Logic.
    *
    * @param inputMap An id and a [[engine.DataType]] in a Map: Map[id, [[engine.DataType]]].
    * @return An id and a [[engine.DataType]] in a Map: Map[id, [[engine.DataType]]].
    */
  override def process(inputMap: Map[String, engine.DataType]): Map[String, engine.DataType] = {
    val inputTable: List[String] = List("HiveTableInput1", "HiveTableInput2")

    // Example use inputMap with a case
    val outputCase: Map[String, engine.DataType] = inputMap.map({
      case ("testDelete", _) => Some("testDelete" -> List("world.txt").toDF)
      case (x: String, y: engine.DataType) if !inputTable.contains(x) => Some(x -> y)
      case _ => None
    }).flatten.toMap

    // Example use inputMap with the name of the key
    val outputDirect: Map[String, engine.DataType] =
      Map("HiveTableOutput" -> inputMap("HiveTableInput1")
        .join(inputMap("HiveTableInput2"), inputMap("HiveTableInput2").columns, "full_outer"))

    outputCase ++ outputDirect
  }
}

/** The Main object to call. */
object Main {

  /**
    *  The main project function.
    *  It run the Spark Application
    *
    * @param args String array with the configuration parameters.
    * @return Nothing.
    */
  def main(args: Array[String]): Unit = {
    SparkBaseApp.design(args).build[SparkArchetype] { app: SparkArchetype =>
      app.exec()
    }
  }
}
