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

package com.bbva.ebdm.ocelot.templates.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, explode, lit, lower, split}

class SparkExampleApp extends SparkBaseApp {

  override def process(inputMap: Map[String, DataFrame]): Map[String, DataFrame] = {

    // Read from Hive
    val hiveWordCount: DataFrame = env.table("word.previouscount")

    // Read from HDFS
    val quijote: DataFrame = env.read.format("text").load("/tmp/file/elQuijote.txt")

    // Ordered word count from file
    val counts = quijote
      .withColumn("value", explode(split(col("value"), "[ )(,;.:-_?¿!|¡\n]")))
      .withColumn("value", lower(col("value")))
      .filter(col("value") =!= "")
      .withColumn("count", lit(1))
      .groupBy("value")
      .sum("count")
      .orderBy(desc("sum(count)"))

    // Union word count from hive with actual word count of the file
    val union = counts.union(hiveWordCount).orderBy(desc("sum(count)"))

    // Get top two words (one "fake" one obtained from Hive and the other a real one obtained from file)
    val over300 = union.where(col("sum(count)") > 300)

    Map( "over300" -> over300 )
  }
}

object SparkExampleAppMain {
  def main(args: Array[String]): Unit = {
    SparkBaseApp.design(args).build[SparkExampleApp] { app: SparkExampleApp => app.exec() }
  }
}