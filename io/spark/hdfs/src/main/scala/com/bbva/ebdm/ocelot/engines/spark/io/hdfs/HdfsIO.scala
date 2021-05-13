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

package com.bbva.ebdm.ocelot.engines.spark.io.hdfs

import com.typesafe.config.{Config, ConfigException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.{ConfigOps, SettingsOps, StringOps}
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine.{DataType, EnvType, Input, Output}
import com.bbva.ebdm.ocelot.engines.spark.io.hdfs.HdfsIO.getHdfs

/**
  *  Create [[com.bbva.ebdm.ocelot.core.Engine.Input]] Subclass.
  *
  * @param formatStr String with format. (json, csv ...)
  * @param optMap Map with configuration options. (key, value)
  * @param schemaStr Some String with schema. (eg: field1 INT, field2 STRING)
  */
case class HdfsInput(path: Seq[String],
  formatStr: String,
  optMap: Option[Map[String, String]] = None,
  schemaStr: Option[String] = None
  ) extends Input with LogSupport {

    /**
      *  Read from HDFS and generate a [[DataType]].
      *
      * @param env An implicit [[EnvType]]
      * @return [[DataType]] data read from HDFS
      */
    override def read()(implicit env: EnvType): DataType = {
      val opts = optMap.getOrElse(Map("" -> ""))
      val df = env
        .read
        .format(formatStr.trim.toLowerCase)
        .options(opts)

      val dfSchema = schemaStr match {
        case Some(x) => df.schema(x)
        case None => df
      }

      debug(s"Loading HdfsInput with options=$opts ")
      dfSchema.load(path: _*)
    }
}

/**
  *  Create [[com.bbva.ebdm.ocelot.core.Engine.Output]] Subclass.
  *
  * @param path String with a [prefix or full] path to write in HDFS.
  * @param subtype String with HDFS subtype.
  * @param formatStr String with format. (json, csv ...)
  * @param optMap Map with configuration options. (key, value)
  * @param partitionBy Some List of String with column partitions. (eg: year)
  */
case class HdfsOutput(
  path: String,
  subtype: String,
  formatStr: Option[String],
  optMap: Option[Map[String, String]],
  partitionBy: Option[Seq[String]]
) extends Output with LogSupport {
  val HdfsPrefix: String = "hdfs://"

  /**
    *  Write to HDFS a [[DataType]].
    *
    * @param data A [[DataType]]
    * @param env An implicit [[EnvType]]
    * @return Nothing.
    */
  override def write(data: DataType)(implicit env: EnvType): Unit = {
    subtype.toLowerCase match {
      case "delete" => delete(data)
      case "file" => file(data)
      case "string" => string(data)
      case "row" => row(data)
    }
  }

  /**
    *  Delete all the files in HDFS in the Strings inside [[DataType]].
    *
    * @param data A [[DataType]]
    * @param env An implicit [[EnvType]]
    * @return Nothing.
    */
  private final def delete(data: DataType)(implicit env: EnvType): Unit = {
    val wholePaths: Array[String] = data.collect().map({ case Row(y: String) => path + y})
    debug(s"Deleting ${wholePaths.length} files from Hdfs")
    val dfs = getHdfs(env)
    wholePaths.foreach(deleteAbsolutePath(dfs, _))
  }

  /**
    *  Create files in HDFS in {{{path + Column "name"}}} inside [[DataType]].
    *
    * @param data A [[DataType]]
    * @param env An implicit [[EnvType]]
    * @return Nothing.
    */
  private final def file(data: DataType)(implicit env: EnvType): Unit = {
    val dfs = getHdfs(env)
    data.select("name", "value").collect().foreach { x =>
      val content: String = x.getString(1)
      val fileName = path + x.getString(0)
      writeContentInFile(content, fileName, dfs)
    }
  }

  /**
    *  Create one files in HDFS in [[path]].
    *
    * @param data A [[DataType]]
    * @param env An implicit [[EnvType]]
    * @return Nothing.
    */
  private final def string(data: DataType)(implicit env: EnvType): Unit = {
    import env.implicits._
    val dfs = getHdfs(env)
    val content = data.as[String].collect().mkString("\n")
    writeContentInFile(content, path, dfs)
  }

  /**
    *  Create files in HDFS in [[path]] inside [[DataType]].
    *
    * @param data A [[DataType]]
    * @return Nothing.
    */
  private final def row(data: DataType): Unit = {
    val frmtStr = formatStr.getOrElse("").trim.toLowerCase
    val opts = optMap.getOrElse(Map("" -> ""))
    val withOption = data.write.options(opts)
    val withPartition = partitionBy match {
      case Some(x) => withOption.partitionBy(x: _*)
      case None => withOption
    }
    val withFormat = withPartition.format(frmtStr.toLowerCase())
    debug(s"Saving Dataframe in HDFS with: format=$frmtStr options=$optMap partitions=$partitionBy path=$path")
    withFormat.save(path)
  }

  /**
    *  Delete all the files in a HDFS [[path]] ++ "file".
    *
    * @param dfs FileSystem
    * @param file String with HDFS name.
    * @return Nothing.
    */
  private final def deleteAbsolutePath(dfs: FileSystem, file: String): Unit = {
    val recursive = false
    val absolutePath = HdfsPrefix + file
    debug(s"Deleting in hdfs: $absolutePath")
    if (!dfs.delete(new Path(absolutePath), recursive)) {
      warn(s"Path $absolutePath could not be successfully deleted")
    }
  }

  /**
    *  Delete all the files in a HDFS [[path]] ++ "file".
    *
    * @param content String to write in HDFS
    * @param completePath String with HDFS path.
    * @param dfs FileSystem
    * @return Nothing.
    */
  private final def writeContentInFile(content: String, completePath: String, dfs: FileSystem): Unit = {
    debug(s"Write in hdfs to file: $completePath")
    val os = dfs.create(new Path(HdfsPrefix + completePath))
    os.write(content.getBytes)
    os.hsync()
  }
}


/** Factory to create [[com.bbva.ebdm.ocelot.core.Engine.IO]] Subclasses. */
object HdfsIO extends LogSupport {

  val validFormats: Seq[String] = Seq("csv", "parquet", "orc", "text", "jdbc", "json")

  /**
    *  Tests whether [[validFormats]] contains a given String as an element.
    *
    * @param frm  String to test.
    *  @return     `true` if this $coll has an element that is equal (as
    *              determined by `==`) to `elem`, `false` otherwise.
    */
  def isValidFormat(frm: String): Boolean = validFormats.contains(frm.trim.toLowerCase)

  /**
    *  Initializing HDFS Service.
    *
    * @param spark A [[EnvType]]
    *  @return      [[FileSystem]]
    */
  def getHdfs(spark: EnvType): FileSystem = {
    // Spark MUST be able to connect to HDFS
    debug("Initializing HDFS Service")
    FileSystem.get(spark.sparkContext.hadoopConfiguration)
  }


  /**
    *  Resolve Configuration to [[com.bbva.ebdm.ocelot.core.Engine.Input]] Subclass.
    *
    * @param k A Configuration.
    * @return [[com.bbva.ebdm.ocelot.engines.spark.io.hdfs.HdfsInput]].
    */
  final def createInput(k: Config): HdfsInput = {
    val path = k.at("path".path[Seq[String]]).getOrFail("Missing path in Hdfs Input") match {
      case x if x.nonEmpty => x
      case _ => throw new ConfigException.Generic("Path in Hdfs Input is empty")
    }
    val formatStr = k.at("formatStr".path[String]).getOrFail("Missing formatStr in Hdfs Input")
    val optMap = k.at("optMap".path[Map[String, String]]).toOption
    val schemaStr = k.at("schemaStr".path[String]).toOption

    if (isValidFormat(formatStr)) {
      debug(s"Creating HdfsInput with: path=$path formatStr=$formatStr optMap=$optMap schemaStr=$schemaStr")
      HdfsInput(path, formatStr, optMap, schemaStr)
    } else {
      val id = k.at("id".path[String]).getOrElse("")
      error(s"Parameter formatStr=$formatStr in HdfsInput with id=$id is not a valid format")
      throw new ConfigException.Generic(s"Parameter formatStr=$formatStr in HdfsInput is not a valid format")
    }

  }

  /**
    *  Resolve Configuration to [[com.bbva.ebdm.ocelot.core.Engine.Output]] Subclass.
    *
    * @param k A Configuration.
    * @return [[com.bbva.ebdm.ocelot.engines.spark.io.hdfs.HdfsOutput]].
    */
  final def createOutput(k: Config): HdfsOutput = {
    val validSubtypes: Seq[String] = Seq("delete", "file", "string", "row")

    val subtype = k.at("subtype".path[String]).getOrFail("Missing subtype in Hdfs Output")
    if (!validSubtypes.contains(subtype.toLowerCase)) throw new ConfigException.Generic(s"Invalid Subtype in Hdfs Output: $subtype")
    val path: String = k.at("path".path[String]).getOrFail(s"Missing path in Hdfs $subtype Output")
    val optionFormatStr: Option[String] =  if (subtype.toLowerCase == "row") {
      val frmt = k.at("formatStr".path[String]).getOrFail(s"Missing formatStr in Hdfs row Output")
      if (isValidFormat(frmt)) {
        Some(frmt)
      } else {
        error(s"Parameter formatStr=$frmt in HdfsOutput Row is not a valid format")
        throw new ConfigException.Generic(s"Parameter formatStr=$frmt in HdfsOutput Row is not a valid format")
      }
    } else {
      None
    }
    val optMap = k.at("optMap".path[Map[String, String]]).toOption
    val partitionBy = k.at("partitionBy".path[Seq[String]]).toOption

    debug(s"Creating HdfsOutput with: subtype=$subtype path=$path optionFormatStr=$optionFormatStr " +
      s"optMap=$optMap partitionBy=$partitionBy")
    HdfsOutput(path, subtype, optionFormatStr, optMap, partitionBy)
  }

}
