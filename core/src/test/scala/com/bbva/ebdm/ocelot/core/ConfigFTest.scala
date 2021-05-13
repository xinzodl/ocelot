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

import java.net.InetAddress
import java.time.Duration
import java.util.UUID
import scala.collection.immutable
import scala.util.matching.Regex

import cats.implicits.catsSyntaxValidatedIdBinCompat0
import com.typesafe.config.{Config, ConfigFactory, ConfigMemorySize}
import org.scalatest.{FunSpec, Matchers}
import wvlet.log.LogSupport

class ConfigFTest extends FunSpec with Matchers with LogSupport {
  val config: Config = ConfigFactory.load()

  describe("path objects") {
    it("should create typed paths") {
      val path = "ints.fortyTwo".path[Int]
      path shouldBe a [ConfigPath[_]]
    }

    it("should create validated typed paths") {
      val path: ConfigPath[Int] = "ints.fortyTwo".path[Int].validate((_: Int) == 42, "Invalid value")
      path shouldBe a [ConfigPath[_]]
    }
  }

  describe("validation") {
    it("should return an invalid value for a nonexistent path") {
      val invalidPath = "ints.thisDoesNotExist".path[Int]
      config.at(invalidPath).isInvalid shouldBe true
    }

    it("should return a valid value for a path that passes validation") {
      val validPath = "ints.fortyTwo".path[Int].validate((_: Int) == 42, "Invalid value")
      config.at(validPath).isValid shouldBe true
    }

    it("should return an invalid value for a path that does not pass validation") {
      val invalidPath = "ints.fortyTwo".path[Int].validate((_: Int) == 40, "Invalid value")
      config.at(invalidPath).isInvalid shouldBe true
    }
  }

  describe("base configuration types") {
    it("should support integers") {
      config.at("ints.fortyTwo".path[Int]) shouldBe 42.validNec
      config.at("ints.fortyTwo".path[Long]) shouldBe 42l.validNec
      config.at("ints.fortyTwo".path[Number]) shouldBe 42.validNec
    }

    it("should support floating point numbers") {
      config.at("floats.pointThirtyThree".path[Double]) shouldBe 0.33.validNec
      config.at("floats.pointThirtyThree".path[Float]) shouldBe 0.33f.validNec
    }

    it("should support booleans") {
      config.at("booleans.trueAgain".path[Boolean]) shouldBe true.validNec
      config.at("booleans.false".path[Boolean]) shouldBe false.validNec
    }

    it("should support strings") {
      config.at("strings.concatenated".path[String]) shouldBe "null bar 42 baz true 3.14 hi".validNec
      config.at("strings.abcdAgain".path[String]) shouldBe "abcd".validNec
    }

    it("should support durations") {
      config.at("durations.halfSecond".path[Duration]) shouldBe Duration.ofMillis(500).validNec
    }

    it("should support sizes") {
      config.at("memsizes.meg".path[ConfigMemorySize]).map(_.toBytes) shouldBe (1024 * 1024).validNec
    }
    it("should support lists of size") {
      config.at("memsizes.megsList".path[Seq[ConfigMemorySize]]).map(_.map(_.toBytes)) shouldBe
        Seq(1024 * 1024,1024 * 1024,1024 * 1024).validNec
    }

    it("should support UUIDs") {
      config.at("extended.uuid".path[UUID]).map(_.version()) shouldBe 4.validNec
      intercept[IllegalArgumentException] {
        config.at("extended.notUuid".path[UUID])
      }
    }

    it("should support InetAddress objects") {
      config.at("extended.addr1".path[InetAddress]).map(_.getHostAddress()) shouldBe "192.168.32.42".validNec
      config.at("extended.addr2".path[InetAddress]).map(_.getHostAddress()) shouldBe "127.0.0.1".validNec
      config.at("extended.addr3".path[InetAddress]).map(_.getHostAddress()) shouldBe "0:0:0:0:0:0:0:1".validNec
      intercept[Exception] {
        config.at("extended.notAddr".path[InetAddress])
      }
    }

    it("should support com.typesafe.config.Config objects") {
      config.at("configs.one".path[Config]) shouldBe ConfigFactory.parseString("""{"a" : "b"}""").validNec
    }

    it("should support Scala maps") {
      config.at("ints".path[immutable.Map[String, AnyRef]]) shouldBe immutable.Map(
        "fortyTwo" -> 42,
        "fortyTwoAgain" -> 42
      ).validNec
    }

    it("should support Scala maps String -> String") {
      config.at("ints".path[immutable.Map[String, String]]) shouldBe immutable.Map(
        "fortyTwo" -> "42",
        "fortyTwoAgain" -> "42"
      ).validNec
    }
  }

  describe("sequence configuration types") {
    it("should support sequences of booleans") {
      config.at("arrays.ofBoolean".path[Seq[Boolean]]) shouldBe Seq(true, false).validNec
    }

    it("should support sequences of ints") {
      config.at("arrays.ofInt".path[Seq[Int]]) shouldBe Seq(1, 2, 3).validNec
    }

    it("should support sequences of longs") {
      config.at("arrays.ofInt".path[Seq[Long]]) shouldBe Seq(1l, 2l, 3l).validNec
    }

    it("should support sequences of numbers") {
      config.at("arrays.ofInt".path[Seq[Number]]) shouldBe Seq(1, 2, 3).validNec
    }

    it("should support sequences of doubles") {
      config.at("arrays.ofDouble".path[Seq[Double]]) shouldBe Seq(3.14, 4.14, 5.14).validNec
    }

    it("should support sequences of float") {
      config.at("arrays.ofDouble".path[Seq[Float]]) shouldBe Seq(3.14f, 4.14f, 5.14f).validNec
    }

    it("should support sequences of strings") {
      config.at("arrays.ofString".path[Seq[String]]) shouldBe Seq("a", "b", "c").validNec
    }

    it("should support sequences of durations") {
      config.at("durations.secondsList".path[Seq[Duration]]) shouldBe
        Seq(1l, 2l, 3l, 4l).map(Duration.ofSeconds).validNec
    }

    it("should support sequences of InetAddress objects") {
      config.at("extended.addresses".path[Seq[InetAddress]]) shouldBe
        Seq("192.168.32.42", "0:0:0:0:0:0:0:1").map(InetAddress.getByName).validNec
    }

    it("should support sequences of com.typesafe.config.Config objects") {
      config.at("configs.list".path[Seq[Config]]) shouldBe
        Seq(
          ConfigFactory.parseString("""{"a" : "b"}"""),
          ConfigFactory.parseString("""{"c" : "d"}""")
        ).validNec
    }

    it("should support sequences of Scala maps") {
      config.at("arrays.ofObject".path[Seq[immutable.Map[String, AnyRef]]]) should contain
      immutable.Map(
        "fortyTwo" -> 42,
        "fortyTwoAgain" -> 42
      ).validNec
    }
  }

  describe("miscellaneous features") {
    val config = ConfigFactory.load()

    it("should allow custom types") {
      case class PhoneNumber(countryCode: Int, areaCode: Int, exchange: Int, extension: Int)
      implicit object PhoneReader extends StringReader[PhoneNumber] {
        val pat: Regex = "(\\d)-(\\d+)-(\\d+)-(\\d+)".r

        def apply(valueStr: String): PhoneNumber = {
          val pat(cc, ac, ex, et) = valueStr
          PhoneNumber(cc.toInt, ac.toInt, ex.toInt, et.toInt)
        }
      }

      config.at("my.phone".path[PhoneNumber]).map(_.extension) shouldBe 1212.validNec
    }

    it("should allow custom types with config") {
      case class PhoneNumber2(countryCode: Int, areaCode: Int, exchange: Int, extension: Int)
      implicit object PhoneReader2 extends ConfigReader[PhoneNumber2] {
        val pat: Regex = "(\\d)-(\\d+)-(\\d+)-(\\d+)".r

        def apply(valueConf: Config): PhoneNumber2 = {
          val pat(cc, ac, ex, et) = valueConf.at("phone".path[String]).toOption.get
          PhoneNumber2(cc.toInt, ac.toInt, ex.toInt, et.toInt)
        }
      }

      config.at("my".path[PhoneNumber2]).map(_.extension) shouldBe 1212.validNec
    }
    it("should allow custom seq of types with config") {
      case class PhoneNumber3(countryCode: Int, areaCode: Int, exchange: Int, extension: Int)
      implicit object PhoneReader3 extends ConfigReader[PhoneNumber3] {
        val pat: Regex = "(\\d)-(\\d+)-(\\d+)-(\\d+)".r

        def apply(valueConf: Config): PhoneNumber3 = {
          val pat(cc, ac, ex, et) = valueConf.at("phone".path[String]).toOption.get
          PhoneNumber3(cc.toInt, ac.toInt, ex.toInt, et.toInt)
        }
      }
      config.at("my2".path[Seq[PhoneNumber3]]).map(_.map(_.extension)) shouldBe List(1212).validNec
    }
  }

  describe("Force Errors") {
    it("should create ConfigF ConfigFApplicative ap") {
      ConfigF.ConfigFApplicative.ap[String, Int] (
        ConfigF.ConfigFApplicative.pure((_: String) => 0)
      )(
        ConfigF.ConfigFApplicative.pure("")
      ).settings(configMonoid.empty) shouldBe 0.validNec
    }

    it("should not create ConfigF ConfigFFunctor map") {
      ConfigF.ConfigFFunctor.map[String, Int] (
        ConfigF.ConfigFApplicative.pure("")
      )(
        (_: String) => 0
      ).settings(configMonoid.empty) shouldBe 0.validNec
    }
  }
}
