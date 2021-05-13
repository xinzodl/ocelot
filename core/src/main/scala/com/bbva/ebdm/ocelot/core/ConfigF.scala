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

package com.bbva.ebdm.ocelot.core

import java.time.Duration
import java.util
import java.util.Map.Entry
import java.util.{Set => JSet}
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaSetConverter}
import scala.collection.immutable

import cats.data.NonEmptyChain
import cats.implicits.catsSyntaxValidatedIdBinCompat0
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.apply.catsSyntaxApply
import cats.{Applicative, Functor}
import com.typesafe.config.{Config, ConfigException, ConfigMemorySize, ConfigValue}

sealed trait ConfigF[A] {
  def settings(config: Config): Settings[A]
}

/**
  *  Implicit conversions and helpers for [[com.bbva.ebdm.ocelot.core.ConfigF]] instances.
  *
  *  package.scala should be used to call the ConfigF object
  *  {{{
  *  import com.bbva.ebdm.ocelot.core._
  *  val example = k.at("example".path[Int]).toOption
  *  }}}
  */
object ConfigF {
  implicit val ConfigFFunctor: Functor[ConfigF] =
    new Functor[ConfigF] {
      def map[A, B](fa: ConfigF[A])(f: A => B): ConfigF[B] =
        new ConfigF[B] {
          def settings(config: Config): Settings[B] =
            fa.settings(config) map f
        }
    }

  implicit val ConfigFApplicative: Applicative[ConfigF] =
    new Applicative[ConfigF] {
      def pure[A](a: A): ConfigF[A] =
        new ConfigF[A] {
          def settings(config: Config): Settings[A] =
            try a.validNec catch {
              case e: ConfigException => e.invalidNec
            }
        }

      def ap[A, B](f: ConfigF[A => B])(fa: ConfigF[A]): ConfigF[B] =
        new ConfigF[B] {
          def settings(config: Config): Settings[B] =
            try f.settings(config) <*> fa.settings(config) catch {
              case e: ConfigException => e.invalidNec
            }
        }
    }

  private def atPath[A](f: Config => String => A): ConfigF[String => A] =
    new ConfigF[String => A] {
      def settings(config: Config): Settings[String => A] =
        f(config).pure[ConfigF].settings(config)
    }

  implicit class EntrySetOps(entrySet: JSet[Entry[String, ConfigValue]]) {
    def asMap: immutable.Map[String, AnyRef] =
      entrySet.asScala.map(e => (e.getKey, e.getValue.unwrapped())).toMap
    def asMapSeqString: Map[String, Seq[String]] =
      entrySet.asScala.map(e =>
        (e.getKey, e.getValue.unwrapped().asInstanceOf[util.ArrayList[String]].asScala)
      ).toMap
  }

  implicit val BooleanAtPath: ConfigF[String => Boolean] =
    atPath(config => path => config.getBoolean(path))
  implicit val BooleanSeqAtPath: ConfigF[String => Seq[Boolean]] =
    atPath(config => path => config.getBooleanList(path).asScala.map(Boolean.unbox))
  implicit val DoubleAtPath: ConfigF[String => Double] =
    atPath(config => path => config.getDouble(path))
  implicit val DoubleSeqAtPath: ConfigF[String => Seq[Double]] =
    atPath(config => path => config.getDoubleList(path).asScala.map(Double.unbox))
  implicit val FloatAtPath: ConfigF[String => Float] =
    atPath(config => path => config.getDouble(path).toFloat)
  implicit val FloatSeqAtPath: ConfigF[String => Seq[Float]] =
    atPath(config => path => config.getDoubleList(path).asScala.map(Double.unbox).map(_.toFloat))
  implicit val IntAtPath: ConfigF[String => Int] =
    atPath(config => path => config.getInt(path))
  implicit val IntSeqAtPath: ConfigF[String => Seq[Int]] =
    atPath(config => path => config.getIntList(path).asScala.map(Int.unbox))
  implicit val LongAtPath: ConfigF[String => Long] =
    atPath(config => path => config.getLong(path))
  implicit val LongSeqAtPath: ConfigF[String => Seq[Long]] =
    atPath(config => path => config.getLongList(path).asScala.map(Long.unbox))
  implicit val NumberAtPath: ConfigF[String => Number] =
    atPath(config => path => config.getNumber(path))
  implicit val NumberSeqAtPath: ConfigF[String => Seq[Number]] =
    atPath(config => path => config.getNumberList(path).asScala)
  implicit val StringAtPath: ConfigF[String => String] =
    atPath(config => path => config.getString(path))
  implicit val StringSeqAtPath: ConfigF[String => Seq[String]] =
    atPath(config => path => config.getStringList(path).asScala)
  implicit val DurationAtPath: ConfigF[String => Duration] =
    atPath(config => path => config.getDuration(path))
  implicit val DurationSeqAtPath: ConfigF[String => Seq[Duration]] =
    atPath(config => path => config.getDurationList(path).asScala)
  implicit val MemorySizeAtPath: ConfigF[String => ConfigMemorySize] =
    atPath(config => path => config.getMemorySize(path))
  implicit val MemorySizeSeqAtPath: ConfigF[String => Seq[ConfigMemorySize]] =
    atPath(config => path => config.getMemorySizeList(path).asScala)
  implicit val ConfigAtPath: ConfigF[String => Config] =
    atPath(config => path => config.getConfig(path))
  implicit val ConfigSeqAtPath: ConfigF[String => Seq[Config]] =
    atPath(config => path => config.getConfigList(path).asScala)
  implicit val MapAtPath: ConfigF[String => immutable.Map[String, AnyRef]] =
    atPath(config => path => config.getConfig(path).entrySet.asMap)
  implicit val MapSeqAtPath: ConfigF[String => Seq[immutable.Map[String, AnyRef]]] =
    atPath(config => path => config.getConfigList(path).asScala.map(_.entrySet.asMap))
  implicit val StringMapAtPath: ConfigF[String => immutable.Map[String, String]] =
    atPath(config => path => config.getConfig(path).entrySet.asMap.mapValues(_.toString))
  implicit val StringMapSeqAtPath: ConfigF[String => Seq[immutable.Map[String, String]]] =
    atPath(config => path => config.getConfigList(path).asScala.map(_.entrySet.asMap.mapValues(_.toString)))
  implicit val MapStringSeqAtPath: ConfigF[String => immutable.Map[String, Seq[String]]] =
    atPath(config => path => config.getConfig(path).entrySet.asMapSeqString)

  implicit def StringReaderAtPath[T](implicit reader: StringReader[T]): ConfigF[String => T] =
    atPath(config => path => reader(config.getString(path)))
  implicit def StringReaderSeqAtPath[T](implicit reader: StringReader[T]): ConfigF[String => Seq[T]] =
    atPath(config => path => config.getStringList(path).asScala.map(reader.apply))

  implicit def ConfigReaderAtPath[T](implicit reader: ConfigReader[T]): ConfigF[String => T] =
    atPath(config => path => reader(config.getConfig(path)))
  implicit def ConfigReaderSeqAtPath[T](implicit reader: ConfigReader[T]): ConfigF[String => Seq[T]] =
    atPath(config => path => config.getConfigList(path).asScala.map(reader.apply))
}

sealed trait ConfigPath[A] extends ConfigF[A] { self =>
  def validate(f: A => Boolean, message: String): ConfigPath[A] =
    new ConfigPath[A] {
      def settings(config: Config): Settings[A] =
        self.settings(config).ensure(NonEmptyChain(new ConfigException.Generic(message)))(f)
    }
}

object ConfigPath {
  def fromString[A](value: String)(implicit atPath: ConfigF[String => A]): ConfigPath[A] =
    new ConfigPath[A] {
      def settings(config: Config): Settings[A] =
        try atPath.settings(config) <*> value.pure[ConfigF].settings(config) catch {
          case e: ConfigException => e.invalidNec
        }
    }
}
