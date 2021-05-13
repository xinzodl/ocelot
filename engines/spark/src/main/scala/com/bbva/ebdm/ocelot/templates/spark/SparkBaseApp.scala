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

package com.bbva.ebdm.ocelot.templates.spark

import wvlet.airframe.Design

import com.bbva.ebdm.ocelot.core.{ConfigOps, StringOps}
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine
import com.bbva.ebdm.ocelot.templates.base.BaseApp

/** Call read data, the Business Logic on the data and finally write the data with Spark. */
abstract class SparkBaseApp extends { val engine = SparkEngine } with BaseApp {

  /**
    *  Call [[com.bbva.ebdm.ocelot.core.Engine.Input]] read, the Business Logic [[com.bbva.ebdm.ocelot.templates.base.BaseApp]] process and finally [[com.bbva.ebdm.ocelot.core.Engine.Output]] write.
    *
    * @return Nothing.
    */
  override def exec(): Unit = {
    info(s"Input list=$inputs")
    info(s"Output list=$outputs")

    val executionTimeInMS = config.at("executionTimeInMS".path[Int]).toOption
    val outputList: Map[String, engine.DataType] = process(inputs.mapValues( _.read() ))
    outputList.foreach {
      case(name, df) =>
        info(s"Persisting DataFrame with id $name in SparkBaseApp")
        outputs(name).write(df)
    }
    if (executionTimeInMS.isDefined) {
      info("Running spark structured streaming app")
      debug(s"Streaming Spark execution with timeout=${executionTimeInMS.get} milliseconds")
      if (executionTimeInMS.get != 0) {
       env.streams.awaitAnyTermination(executionTimeInMS.get)
      }
      else {
        env.streams.awaitAnyTermination()
      }
    }
  }
}

/** Factory for [[com.bbva.ebdm.ocelot.templates.spark.SparkBaseApp]] Design. */
object SparkBaseApp {

  /**
    *  Create a Design calling [[com.bbva.ebdm.ocelot.templates.base.BaseApp]] Design.
    *
    * @param args String array with the configuration parameters.
    * @return A Design.
    */
  def design(args: Array[String]): Design = BaseApp.design(args)
}
