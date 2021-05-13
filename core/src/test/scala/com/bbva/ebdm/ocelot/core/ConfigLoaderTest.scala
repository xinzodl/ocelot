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

import java.io.File
import java.nio.file.{Path, Paths}

import cats.implicits._
import com.typesafe.config.{Config, ConfigException}
import org.scalatest._
import wvlet.log.LogSupport

class ConfigLoaderTest extends FunSpec with Matchers with LogSupport {
  val validArgs = Array("-Dthree=3","-DparameterTwo=this.is.two","-Dparameter.one=1", "-Dints.fortyTwo=100",
     "-Dappname=The_App_Name_is_${name}")
  val invalidArgs = Array("one=1")

  val config: Config = ConfigLoader.load(validArgs)

  describe("fails"){
    it("should fail on wrong parameter format"){
      assertThrows[ConfigException.Generic] {
        ConfigLoader.load(invalidArgs)
      }
    }
  }

  describe("conversions"){
    it("should convert to Map[String, Seq[String]]"){
      val fin: Map[String, Seq[String]] = Map("impar" -> Seq("uno","tres"), "par" -> Seq("dos","cuatro"))
      config.at("mapstringseq".path[Map[String, Seq[String]]]) shouldBe fin.validNec
    }
    it("should convert to Seq[Map[String, String]]"){
      val fin: Seq[Map[String, String]] = Seq(Map("here" -> "i come"), Map("so" -> "beware"))
      config.at("seqofmaps".path[Seq[Map[String, String]]]) shouldBe fin.validNec
    }
    it("should convert to another Map[String, Seq[String]]"){
      val fin = Map("common" -> Seq("1", "2", "3", "4", "5", "6", "7", "8", "9"),
        "deltair" -> Seq("10"),
        "cred" -> Seq("11"),
        "fx" -> Seq("12"),
        "vegair" -> Seq("13"))
      config.at("loadMasks".path[Map[String, Seq[String]]]) shouldBe fin.validNec
    }
   it("shuould convert to Seq[Int]"){
     val fin = Seq(370)
     config.at("outputGroups".path[Seq[Int]]) shouldBe fin.validNec
   }
  }

  describe("parameter variables") {
    it("should get config variables from args") {
      config.at("three".path[Int]) shouldBe 3.validNec
      config.at("parameterTwo".path[String]) shouldBe "this.is.two".validNec
      config.at("parameter.one".path[Int]) shouldBe 1.validNec
    }
  }

  describe("load values"){
    it ("should load system values"){
      config.at("system.file.separator".path[String]) shouldBe "/".validNec
    }

    it ("should load cluster values"){
      config.at("cluster.name".path[String]) shouldBe "DefaultClusterName".validNec
    }

    it ("should load default values"){
      config.at("defaultValue".path[String]) shouldBe "this is a default value".validNec
    }

    it ("should load application values"){
      config.at("name".path[String]) shouldBe "MyDefaultAppName".validNec
    }

    it("should support system properties") {
      config.at("system.java.runtime.name".path[String])
        .map(_ should (include ("Java") or include ("OpenJDK")))
        .getOrElse(fail)
    }

    it("should support paths and files") {
      config.at("system2.userhome".path[Path]) shouldBe Paths.get(sys.props("user.home")).validNec
      config.at("system2.userhome".path[File]) shouldBe Paths.get(sys.props("user.home")).toFile.validNec
    }
  }

  describe("override values"){
    it("should override any config values with parameters"){
      val newValidArgs = Array("-Dthree=3","-DparameterTwo=this.is.two","-Dparameter.one=1",
        "-Dappname=The App Name is ${name}")
      val originalConfig = ConfigLoader.load(newValidArgs)
      originalConfig.at("ints.fortyTwo".path[Int]) shouldBe 42.validNec
      config.at("ints.fortyTwo".path[Int]) shouldBe 100.validNec
    }

    it("application.conf should override all except parsed args"){
      config.at("info".path[String]) shouldBe "This is the application info".validNec
    }

    it("defaults.conf should override system and cluster"){
      config.at("system.user.country".path[String]) shouldBe "MY made up country".validNec
    }

    it("system should override cluster.conf"){
      config.at("system.line.separator".path[String]) shouldBe "\n".validNec
    }
  }

  describe("cross references"){
    it("should be possible to have cross refernece on args, application.conf and defualts.conf"){
      config.at("appname".path[String]) shouldBe "The_App_Name_is_MyDefaultAppName".validNec
      config.at("appnamefromdefaults".path[String]) shouldBe "MyDefaultAppName".validNec
    }
  }

  describe("should resolveWith with existing config"){
    it("should allow string interpolation") {
      val query = "SELECT count(*) FROM ${hqlvalues.db}.${hqlvalues.table} LIMIT ${hqlvalues.limit};"
      query.resolveString(config) shouldBe "SELECT count(*) FROM alvaro.test_table LIMIT 100;"

      val query2 = "SELECT count(*) FROM alvaro.test_table LIMIT ${hqlvalues.key1.key2}"
      query2.resolveString(config) shouldBe "SELECT count(*) FROM alvaro.test_table LIMIT 255"
    }
  }
}
