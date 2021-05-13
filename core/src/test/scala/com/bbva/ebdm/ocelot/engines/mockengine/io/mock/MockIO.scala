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

package com.bbva.ebdm.ocelot.engines.mockengine.io.mock

import com.typesafe.config.Config

import com.bbva.ebdm.ocelot.engines.mockengine.MockEngine._

case class in(x: String) extends Input {
  override def read()(implicit env: String): String = ""
}
case class out(x: String) extends Output {
  override def write(data: String)(implicit env: String): Unit = Unit
}

object MockIO {
  final def createInput(k: Config): in = {
    in("this is a mockengine INput ")
  }

  final def createOutput(k: Config): out = {
    out("this is a mockengine OUTput")
  }
}
