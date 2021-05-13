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

package com.bbva.ebdm.ocelot.engines.spark.io.hive

import com.typesafe.config.{Config, ConfigException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, expr, input_file_name, lit}
import org.apache.spark.sql.{Column, SaveMode}
import wvlet.log.LogSupport

import com.bbva.ebdm.ocelot.core.{ConfigOps, SettingsOps, StringOps}
import com.bbva.ebdm.ocelot.engines.spark.SparkEngine.{DataType, EnvType, Input, Output}

/**
  *  Create [[com.bbva.ebdm.ocelot.core.Engine.Input]] Subclass.
  *
  * @param schema String with database name.
  * @param table String with table name.
  * @param fields Some List of String with the selected columns.
  * @param filters Some Partitions to filter.
  * @param extraFilters Some Columns to filter.
  */
case class HiveInput(
  schema: String,
  table: String,
  fields: Option[Seq[String]] = None,
  filters: Option[Partitions] = None,
  extraFilters: Option[Column] = None
) extends Input with LogSupport {

  /**
    *  Read from Hive and generate a [[DataType]].
    *
    * @param env An implicit [[EnvType]]
    * @return [[DataType]].
    */
  override def read()(implicit env: EnvType): DataType = {
    debug(s"Read DataFrame from hive with: schema:$schema table=$table fields=$fields filters=$filters extraFilters=$extraFilters")
    val selectFields = fields.getOrElse(Seq("*")).map( x => col(x))
    env
      .table(s"$schema.$table")
      .select(selectFields:_*)
      .where(extraFilters.getOrElse(lit(true)))
      .filterByPartitions(filters)
  }
}

/**
  *  Create [[com.bbva.ebdm.ocelot.core.Engine.Input]] Subclass.
  *
  * @param schema String with database name.
  * @param table String with table name.
  * @param writeMode SaveMode with the write Save Mode.
  * @param overridePartitions Boolean if override partitions.
  * @param repartition Some Int with the numbre of repartitions.
  * @param overrideHdfsPartitions Some List of Strings with Columns name to delete in HDFS the partitions of this Columns.
  */
case class HiveOutput(
  schema: String,
  table: String,
  writeMode: SaveMode = SaveMode.Append,
  overridePartitions: Boolean = false,
  repartition: Option[Int] = None,
  overrideHdfsPartitions: Option[List[String]] = None
) extends Output with LogSupport {

  /**
    *  Write to Hive a [[DataType]].
    *
    * @param data A [[DataType]]
    * @param env An implicit [[EnvType]]
    * @return Nothing.
    */
  override def write(data: DataType)(implicit env: EnvType): Unit = {
    debug(s"Persist Dataframe to hive with: schema=$schema table=$table writeMode=$writeMode overridePartitions=$overridePartitions " +
      s"repartition=$repartition overrideHdfsPartitions=$overrideHdfsPartitions")
    val fullTableName = s"$schema.$table"
    val repartitionedData = if (repartition.isDefined) data.repartition(repartition.get) else data
    val mode: SaveMode = if (overrideHdfsPartitions.isDefined) {
      deleteHdfsPartition(repartitionedData, overrideHdfsPartitions.get, fullTableName)
      debug("Since overrideHdfsPartitions is defined, setting SaveMode = Append")
      SaveMode.Append
    } else if (overridePartitions) {
      debug("Since overridePartitions is true, setting 'spark.sql.sources.partitionOverwriteMode' to 'dynamic'" +
        "and SaveMode to Overwrite")
      env.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      SaveMode.Overwrite
    } else {
      debug("Since overridePartitions is false, setting 'hive.exec.dynamic.partition' to 'true'" +
        "and 'hive.exec.dynamic.partition.mode' to 'nonstrict'")
      env.conf.set("hive.exec.dynamic.partition", "true")
      env.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      writeMode
    }
    repartitionedData.write.mode(mode).insertInto(fullTableName)
  }

  /**
    *  Delete all the files in a HDFS Partition.
    *
    * @param df A [[DataType]]
    * @param partitionList List of Strings with Columns name to delete in HDFS the partitions of this Columns.
    * @param fullTableName String with Database name and table name
    * @return Nothing.
    */
  private def deleteHdfsPartition(df: DataType, partitionList: List[String], fullTableName: String)(implicit env: EnvType): Unit = {
    debug(s"Delete HDFS logical partitions=$partitionList from table=$fullTableName")
    val getPartitions = df // dataframe to be written
      .select(partitionList.head, partitionList.tail:_*) // select dates
      .distinct
      .collect
      .map{r => // For each row, create group of conditions that would get all records with those values (that "partitions")
        partitionList.zipWithIndex.foldLeft(lit(true)){ case(acc, (name, index)) =>
          acc && (col(name) === lit(r.get(index)))}
      }.reduce(_ || _)
    val recordsToDelete = env.table(fullTableName).where(getPartitions) // get all records from "partitions" to be deleted
    val oldFiles = recordsToDelete.select(input_file_name()).distinct.collect.map(x=>x.getAs[String](0))
    debug(s"Deleting ${oldFiles.length} files from hdfs")
    oldFiles.foreach(deleteFiles)
    debug("Deleting files in HDFS done. Table refresh")
    env.sql(s"REFRESH $fullTableName")
  }

  /**
    *  Delete all the files in a HDFS Path.
    *
    * @param absolutePath String with HDFS path.
    * @return Nothing.
    */
  private def deleteFiles(absolutePath: String)(implicit env: EnvType): Unit = {
    debug("Deleting file: " + absolutePath)
    val recursive = false
    try {
      val dfs = FileSystem.get(env.sparkContext.hadoopConfiguration)
      dfs.delete(new Path(absolutePath), recursive)
    } catch {
      case t: Throwable => throw t
    }
  }
}

/** Factory to create [[com.bbva.ebdm.ocelot.core.Engine.IO]] Subclasses. */
object HiveIO extends LogSupport {

  /**
    *  Resolve Configuration to [[com.bbva.ebdm.ocelot.core.Engine.Input]] Subclass.
    *
    * @param k A Configuration.
    * @return [[com.bbva.ebdm.ocelot.engines.spark.io.hive.HiveInput]].
    */
  final def createInput(k: Config): HiveInput = {
    val schema = k.at("schema".path[String]).getOrFail("Missing schema in Hive Input")
    val table = k.at("table".path[String]).getOrFail("Missing table in Hive Input")
    val fields = k.at("fields".path[Seq[String]]).toOption
    val datePartitions = calculatePartitions(k.at("YYYYMMDD".path[Int]).toOption)
    val extraFilters = k.at("extraFilters".path[String]).toOption match {
      case Some(s: String) => Some(expr(s))
      case None => None
    }

    debug(s"Creating HiveInput with: schema=$schema table=$table fields=$fields " +
      s"datePartitions=$datePartitions extraFilters=$extraFilters")
    HiveInput(schema, table, fields, datePartitions, extraFilters)
  }

  /**
    *  Resolve Configuration to [[com.bbva.ebdm.ocelot.core.Engine.Output]] Subclass.
    *
    * @param k A Configuration.
    * @return [[com.bbva.ebdm.ocelot.engines.spark.io.hive.HiveOutput]].
    */
  final def createOutput(k: Config): HiveOutput = {
    val schema = k.at("schema".path[String]).getOrFail("Missing schema in Hive Output")
    val table = k.at("table".path[String]).getOrFail("Missing table in Hive Output")
    val writeMode = SaveMode.valueOf(k.at("writeMode".path[String]).getOrElse("Append"))
    val overridePartitions = k.at("overridePartitions".path[Boolean]).getOrElse(false)
    val repartition = k.at("repartition".path[Int]).toOption
    val overrideHdfsPartitions = k.at("overrideHdfsPartitions".path[Seq[String]]).map(_.toList).toOption

    debug(s"Creating hiveOutput with: schema=$schema table=$table writeMode=$writeMode overridePartitions=$overridePartitions " +
      s"repartition=$repartition overrideHdfsPartitions=$overrideHdfsPartitions")
    HiveOutput(schema, table, writeMode, overridePartitions, repartition, overrideHdfsPartitions)
  }

  /**
    *  Resolve Some Int to Some [[com.bbva.ebdm.ocelot.engines.spark.io.hive.Partitions]].
    *
    * @param a Some Int.
    * @return Some [[com.bbva.ebdm.ocelot.engines.spark.io.hive.Partitions]].
    */
  private final def calculatePartitions(a: Option[Int]): Option[Partitions] = a match {
    case Some(n) if n.toString.length == 4 => Some(Year(n))
    case Some(n) if n.toString.length == 6 => Some(Month(n / 100, n % 100))
    case Some(n) if n.toString.length == 8 => Some(Date(n / 10000, (n / 100) % 100, n % 100))
    case Some(n) => throw new ConfigException.Generic(s"Invalid Date: $n")
    case None => None
  }

}
