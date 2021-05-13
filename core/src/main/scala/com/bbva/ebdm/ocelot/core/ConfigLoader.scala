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

import cats.implicits.catsSyntaxSemigroup
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import wvlet.log.LogSupport

/** Unify all config sources in one Config */
object ConfigLoader extends LogSupport {

  /**
    *  Transform a String array into a Config. Each String must start with "-D".
    *
    * @param args String array with the configuration parameters.
    * @return A Configuration.
    */
  private def parseArgs(args: Array[String]): Config = {
    debug(s"Parsing args=${args.mkString(" ")}")
    val pattern = """-D((\w+\.)*\w+=.*)""".r
    val params = args.map {
      case pattern(pair, _) => pair
      case arg => throw new ConfigException.Generic(s"""unable to parse command-line argument "$arg"""")
    }
    ConfigFactory.parseString(params.mkString(", "))
  }

  /**
    *  Unify all settings in one Config by priority.
    *
    *  From highest to lowest priority:
    *    Param args
    *    Configuration file: "application.conf"
    *    Configuration file: "defaults.conf"
    *    Configuration file: "cluster.conf"
    *    System configuration
    *
    *  When a specific key appears in more than one, it will be overridden by settings with higher priority.
    *
    * @param args String array with the configuration parameters.
    * @return A Configuration.
    */
  def load(args: Array[String]): Config = {
    val system = ConfigFactory.defaultOverrides()
    debug("system configuration loaded: " + system)

    val cluster = ConfigFactory.parseResources("cluster.conf")
    debug("cluster configuration loaded: " + cluster)

    val defaults = ConfigFactory.parseResources("defaults.conf")
    debug("default configuration loaded: " + defaults)

    val app = ConfigFactory.parseResources("application.conf")
    debug("application configuration loaded: " + app)

    val parsedArgs = parseArgs(args)
    debug("command-line arguments configuration loaded: " + parsedArgs)

    val config = (
      parsedArgs
      |+| app
      |+| defaults
      |+| system.atKey("system")
      |+| cluster.atKey("cluster")
      ).resolve()

    info(s"Resolved config=$config")
    config
  }
}
