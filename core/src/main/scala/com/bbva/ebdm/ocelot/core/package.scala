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
 *
 * ---------------------------------------------------------------------------
 *
 * Based on Configz (https://github.com/arosien/configz)
 */

package com.bbva.ebdm.ocelot

import scala.annotation.tailrec

import cats.Monoid
import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNec
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import wvlet.log.Logger

/**
  *  Provides classes for dealing with configurations.  Also provides
  *  implicits for converting configurations to types implemented in ConfigF.
  *
  *  {{{
  *  scala> import com.bbva.ebdm.ocelot.core._
  *  scala> val example = k.at("example".path[Int]).toOption
  *  }}}
  */
package object core {
  type Settings[+A] = ValidatedNec[ConfigException, A]

  /**
    *  A class to create a new SettingsOps[A].
    *
    * @param settings Settings[A].
    */
  implicit class SettingsOps[A](settings: Settings[A]) {

    /**
      *  Resolves the Settings[A] to A.
      *
      * @param message It is a string, only used if an error occurs.
      * @return An object of type A.
      */
    def getOrFail(message: String): A = settings match {
      case Valid(a)    => a
      case Invalid(le) =>
        val logger = Logger.of[Settings[_]]
        for (e <- le.iterator) {
          logger.error(e)
        }
        throw new ConfigException.Generic(message)
    }
  }

  implicit val configMonoid: Monoid[Config] = new Monoid[Config] {

    /**
      *  Creates an empty Config
      *
      * @return An empty Configuration.
      */
    def empty: Config = ConfigFactory.empty

    /**
      *  Combine two Config
      *  Same as: {{{ x.withFallback(y) }}}
      *
      * @param x A Configuration.
      * @param y A Configuration.
      * @return A Configuration.
      */
    def combine(x: Config, y: Config): Config = x.withFallback(y)
  }

  /**
    *  A class to create a new ConfigOps.
    *
    * @param config A Configuration.
    */
  implicit class ConfigOps(config: Config) {

    /**
      *  Resolves ConfigPath[A] to a Settings[A]
      *
      * @param configf ConfigPath[A] containing a path and type to a config value.
      * @return A Settings[A].
      */
    def at[A](configf: ConfigPath[A]): Settings[A] = configf.settings(config)
  }

  /**
    *  A class to create a new StringOps.
    *
    * @param value A String.
    */
  implicit class StringOps(value: String) {

    /**
      *  Resolves String in ConfigPath[A]
      *
      * @param atPath Containing a function which transforms a String to a type A.
      * @return A ConfigPath[A] containing a path and type to a config value.
      */
    def path[A](implicit atPath: ConfigF[String => A]): ConfigPath[A] =
      ConfigPath.fromString(value)


    /**
      * Resolves all values like '${key1[.key2]*}' in a string using config from parameters
      *
      * Tail recursive method which solves one reference with each call
      *
      * @param config Containing config values used to resolve substitutions in current string
      * @return A string with all values resolved using current config
      */
    @tailrec
    final def resolveString(config: Config): String = {
      val regEx = "(.*)\\$\\{([a-zA-Z0-9]*[\\.[a-zA-Z0-9]*]*)\\}(.*)".r
      value match {
        case regEx(pre, key, post) =>
          val str = pre + config.at(key.path[String]).getOrElse("ERROR: Replacing Error") + post
          str.resolveString(config)
        case _ => value
      }
    }
  }
}
