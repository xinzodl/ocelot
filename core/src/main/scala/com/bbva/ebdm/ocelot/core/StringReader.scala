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
 * Based on Simple Scala Config (https://github.com/ElderResearch/ssc)
 */

package com.bbva.ebdm.ocelot.core

import java.io.File
import java.net.InetAddress
import java.nio.file.{Path, Paths}
import java.util.UUID

trait StringReader[T] {
  def apply(valueStr: String): T
}

object StringReader {
  implicit object PathReader extends StringReader[Path] {
    def apply(valueStr: String): Path =
      Paths.get(valueStr)
  }
  implicit object FileReader extends StringReader[File] {
    def apply(valueStr: String): File =
      PathReader(valueStr).toFile
  }
  implicit object UUIDReader extends StringReader[UUID] {
    def apply(valueStr: String): UUID =
      UUID.fromString(valueStr)
  }
  implicit object InetAddrReader extends StringReader[InetAddress] {
    def apply(valueStr: String): InetAddress =
      InetAddress.getByName(valueStr)
  }
}
