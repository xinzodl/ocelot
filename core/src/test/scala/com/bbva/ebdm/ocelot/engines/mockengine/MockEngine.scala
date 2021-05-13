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

package com.bbva.ebdm.ocelot.engines.mockengine

import com.typesafe.config.Config
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.Engine

object MockEngine extends Engine with LogSupport {
  override type EnvType = String
  override type DataType = String

  override def engineName: String = "mockengine"

  override def getEnv(config: Config): String = {
    "getEnv done!"
  }
}
