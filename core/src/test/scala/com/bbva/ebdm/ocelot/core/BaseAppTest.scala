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

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.scalatest.{FunSuite, Matchers}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.engines.mockengine.MockEngine
import com.bbva.ebdm.ocelot.templates.base.BaseApp

class BaseAppTest extends FunSuite with Matchers with LogSupport {

  val fullConfig: Config = ConfigFactory.parseResources("inputOutput.conf").getConfig("BatchInputOutputTest")
  val fullConfig2: Config = ConfigFactory.parseResources("application2.conf").getConfig("BatchInputOutputTest")

  class MyBaseApp(cf: Config) extends {val engine = MockEngine} with BaseApp {
    override def exec(): Unit = Unit
    override lazy val config: Config = cf
    override def process(inputMap: Map[String, String]): Map[String, String] = Map()
  }

  def obj(cf: Config) = new MyBaseApp(cf)

  // ------------------------------- Test input errors -------------------------------
  test("should throw error with Input not specified") {
      the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("missingIN")).inputs should have message
        "Inputs key not specified"
  }
  test("should throw error with Inputs key is empty") {
      the [ConfigException] thrownBy obj(fullConfig.getConfig("emptyIN")).inputs should have message
        "Inputs key is an empty sequence, at least one input is needed"
  }
  test("should throw error with Input type not specified") {
      the [ConfigException] thrownBy obj(fullConfig.getConfig("missingtypeIN")).inputs should have message
        "Input type not specified"
  }
  test("should throw error with Input streaming not created Id") {
    the [ConfigException.Generic] thrownBy obj(fullConfig2.getConfig("streamingnotIN")).inputs should have message
      "Missing id in streaming Input"
  }
  test("should throw error with Invalid Type in Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig2.getConfig("weirdtypeIN")).inputs should have message
      "Invalid Type in Input: papyrus"
  }
  test("should throw error with missing id in hive Input ") {
    the [ConfigException.Generic] thrownBy obj(fullConfig2.getConfig("missingIdINhive")).inputs should have message
      "Missing id in hive Input"
  }
  test("should throw error with missing id in Hdfs Input") {
    the [ConfigException.Generic] thrownBy obj(fullConfig2.getConfig("missingIdINhdfs")).inputs should have message
      "Missing id in hdfs Input"
  }

  // ------------------------------- Test output errors -------------------------------
  test("should throw error with Output not specified") {
    the [ConfigException.Generic] thrownBy obj(fullConfig.getConfig("missingOUT")) should have message
      "Outputs key not specified"
  }
  test("should throw error with Outputs key is empty") {
    the [ConfigException] thrownBy obj(fullConfig.getConfig("emptyOUT")).outputs should have message
      "Outputs key is an empty sequence, at least one output is needed"
  }
  test("should throw error with Output type not specified") {
    the [ConfigException] thrownBy obj(fullConfig.getConfig("missingTypeOUT")).outputs should have message
      "Output type not specified"
  }
  test("should throw error with Output not created Id") {
    the [ConfigException] thrownBy obj(fullConfig.getConfig("missingIdOUT")).outputs should have message
      "Missing id in streaming Output"
  }
  test("should throw error with Invalid Type in Output") {
    the [ConfigException] thrownBy obj(fullConfig.getConfig("weirdtypeOUT")).outputs should have message
      "Invalid Type in Output: papyrus"
  }

  // Exception in IO
  test("should throw error in IO when returning invalid symbols") {
    the [Exception] thrownBy obj(fullConfig.getConfig("exceptionInIoIN")).inputs should have message
      "Invalid Symbol in Inputs returned by: com.bbva.ebdm.ocelot.engines.mockengine.io.mockexception.MockexceptionIO"
  }
  test("should throw error in IO and return message") {
    the [Exception] thrownBy obj(fullConfig.getConfig("exceptionInIo2IN")).outputs should have message
    "This is the exception message"
  }

}
