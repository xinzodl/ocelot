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

package com.bbva.ebdm.ocelot.engines.spark.io

import org.apache.spark.sql.functions.{col, regexp_extract}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
import wvlet.log.LogSupport

/** Provides classes and functions for dealing with configurations. */
package object hive extends LogSupport {

  /**
    *  Get the max partition of a table.
    *
    * @param session Some [[Partitions]].
    * @param schema A String with the Database name.
    * @param table A String with the Table name.
    *  @return A DataFrame.
    */
  def getMaxPartition(session: SparkSession, schema: String, table: String): Array[Int] = {
    debug("Get max partition from: " + schema + "." + table)
    val fullTableName = s"$schema.$table"
    val expr = (id: Int, colName: String) => regexp_extract(col("partition"),"year=(\\d{4})/month=(\\d{1,2})/day=(\\d{1,2}).*", id).as(colName).cast(IntegerType)

    val dfPart = session.sql(s"show partitions $fullTableName")
      .select(expr(1, "year"), expr(2, "month"), expr(3, "day"))
      .orderBy(col("year").desc, col("month").desc, col("day").desc)
    val firstRow = dfPart.first()
    Array(firstRow.getInt(0),firstRow.getInt(1),firstRow.getInt(2))
  }

  /**
    *  A DataFrame who uses our application.
    *
    *  @constructor create a new DataFrame.
    * @param df A DataFrame.
    */
  implicit class DataFramePartitions(df: DataFrame){
    /**
      *  Convert a DataFrame to a filtered DataFrame if Some "partitions".
      *
      * @param partitions Some [[Partitions]].
      *  @return A DataFrame.
      */
    def filterByPartitions[T<: Partitions](partitions: Option[T]):DataFrame = {
      partitions match {
        case Some(x) => x.whereClause(df)
        case None => df
      }
    }
  }

  /**
    *  A trait that should convert a DataFrame to a filtered DataFrame.
    *
    *  @constructor A override function DataFrame => DataFrame.
    *  @return A DataFrame.
    */
  sealed trait Partitions{def whereClause(df: DataFrame): DataFrame}

  /**
    *  A Date who uses our application.
    *
    *  @constructor create a new Date with year, month and day.
    * @param year A Int.
    * @param month A Int.
    * @param day A Int.
    */
  case class Date(year: Int, month: Int, day: Int) extends Partitions {
    /**
      *  Convert a DataFrame to a filtered DataFrame by their Columns year, month, day.
      *
      * @param df A DataFrame.
      *  @return A DataFrame.
      */
    override def whereClause(df: DataFrame): DataFrame =
      df.where(df("year") === year && df("month") === month && df("day") === day)
  }

  /**
    *  A Date who uses our application.
    *
    *  @constructor create a new Date with year and month.
    * @param year A Int.
    * @param month A Int.
    */
  case class Month(year: Int, month: Int) extends Partitions {
    /**
      *  Convert a DataFrame to a filtered DataFrame by their Columns year, month.
      *
      * @param df A DataFrame.
      *  @return A DataFrame.
      */
    override def whereClause(df: DataFrame): DataFrame =
      df.where(df("year") === year && df("month") === month )
  }

  /**
    *  A Date who uses our application.
    *
    *  @constructor create a new Date with year.
    * @param year A Int.
    */
  case class Year(year: Int) extends Partitions {
    /**
      *  Convert a DataFrame to a filtered DataFrame by their Columns year.
      *
      * @param df A DataFrame.
      *  @return A DataFrame.
      */
    override def whereClause(df: DataFrame): DataFrame =
      df.where(df("year") === year)
  }
}
