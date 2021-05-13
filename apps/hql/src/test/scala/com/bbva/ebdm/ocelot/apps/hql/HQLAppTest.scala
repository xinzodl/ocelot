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

package com.bbva.ebdm.ocelot.apps.hql

import scala.sys.process.stringToProcess

import com.typesafe.config.ConfigException
import org.apache.hadoop.fs.FileSystem
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import wvlet.log.LogSupport

class HQLAppTest extends FunSpec with Matchers with BeforeAndAfterAll with LogSupport {
  describe("Build class HQLApp") {
    it("Create Database") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        app.env.sql("CREATE DATABASE IF NOT EXISTS prueba")
        app.env.sql("SHOW DATABASES")
          .collect()
          .map(_ (0))
          .contains("prueba") shouldBe true
      }
    }

    it("Create table") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        app.env.sql("create table if not exists prueba.prueba_test (uno String, dos String)")
        app.env.sql("show tables in prueba")
          .select("tableName")
          .collect()
          .map(_ (0))
          .contains("prueba_test") shouldBe true
      }
    }

    it("Table exist") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        app.env.sql("select * from prueba.prueba_test")
          .columns shouldBe Array("uno", "dos")
      }
    }

    it("Drop table") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        app.env.sql("drop table if exists prueba.prueba_test")
        val dropTabla = app.env.sql("show tables in prueba")
          .select("tableName")
          .collect()
        if (dropTabla.isEmpty) {
          dropTabla shouldBe Array()
        }
        else {
          dropTabla
            .map(_ (0))
            .contains("prueba_test") shouldBe false
        }
      }
    }

    it("Print all querys in aplications.conf") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        app.queriesSeq shouldBe Seq("create table if not exists prueba.prueba_aplication (primer String, segundo String)",
          """insert overwrite table prueba.prueba_aplication values ("0.0","hola")""",
          "select * from prueba.prueba_aplication",
          "ALTER TABLE prueba.prueba_aplication ADD COLUMNS (tercero Double)",
          """insert into prueba.prueba_aplication values ("Mundo","50", 45)""",
          "select * from prueba.prueba_aplication")
      }
    }

    it("Exec") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        app.exec()
        app.env.sql("show tables in prueba")
          .select("tableName")
          .collect()
          .map(_(0))
          .contains("prueba_aplication") shouldBe true
      }
    }

    it("DF table prueba_aplication in exec") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        val salidaDF = app.env.sql("select * from prueba.prueba_aplication")
          .collect()
          .map(x => (x(0), x(1), x(2)))
        salidaDF.contains(("Mundo", "50", 45.0)) shouldBe true
        salidaDF.contains(("0.0", "hola", null)) shouldBe true
        salidaDF.length shouldBe 2
      }
    }

    it("Drop tables exec") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        app.env.sql("drop table if exists prueba.prueba_aplication")
        val dropTabla = app.env.sql("show tables in prueba")
          .select("tableName")
          .collect()
        if (dropTabla.isEmpty) {
          dropTabla shouldBe Array()
        }
        else {
          dropTabla
            .map(_ (0))
            .contains("prueba_aplication") shouldBe false
        }
      }
    }
  }

  describe("Call Main") {
    it("Main") {
      com.bbva.ebdm.ocelot.apps.hql.Main.main(Array())
    }

    it("Querys created in Main") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        app.env.sql("show tables in prueba")
          .select("tableName")
          .collect()
          .map(_ (0))
          .contains("prueba_aplication") shouldBe true
      }
    }

    it("Override queries with args") {
      val args = Array("-Dqueries=[{type=unsupported, content=this is not a valid type}]")
      the [ConfigException.Generic] thrownBy HQLApp.design(args).build[HQLApp] { app: HQLApp =>
         app.exec()
      } should have message "Expected type 'string' or 'file' but found 'unsupported'"
    }

    it("DF table prueba_aplication in Main") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        val salidaDF = app.env.sql("select * from prueba.prueba_aplication")
          .collect()
          .map(x => (x(0), x(1), x(2)))
          .toList
        salidaDF.contains(("Mundo", "50", 45.0)) shouldBe true
        salidaDF.contains(("0.0", "hola", null)) shouldBe true
        salidaDF.length shouldBe 2
      }
    }

    it("Drop table Main") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        app.env.sql("drop table if exists prueba.prueba_aplication")
        val dropTabla = app.env.sql("show tables in prueba")
          .select("tableName")
          .collect()
        if (dropTabla.isEmpty) {
          dropTabla shouldBe Array()
        }
        else {
          dropTabla
            .map(_ (0))
            .contains("prueba_aplication") shouldBe false
        }
      }
    }

    it("Drop Database") {
      HQLApp.design(Array()).build[HQLApp] { app: HQLApp =>
        app.env.sql("DROP DATABASE IF EXISTS prueba")
        val dropDatabase = app.env.sql("SHOW DATABASES")
          .collect()
        if (dropDatabase.isEmpty) {
          dropDatabase shouldBe Array()
        }
        else {
          dropDatabase
            .map(_(0))
            .contains("prueba") shouldBe false
        }
      }
    }
  }

  val dockerName: String = "docker_apps_HQLApp"
  val dockerName_net: String = s"${dockerName}_net"
  val ip: String = "172.26.0.9"
  val subnet: String = ip.substring(0, ip.lastIndexOf(".") + 1) + "0/16"
  val masterPort: String = "7077"
  val slavePort: String = "35261"
  val hdfsPort: String = "9000"
  val dockerImage: String = "ocelot/test:1.0"

  override def beforeAll(): Unit = {
    info("BeforeAll!")
    info("\n--------Create network-----------")
    val createNet = s"docker network create --subnet=$subnet $dockerName_net".!!
    info(createNet)

    info("\n--------Docker run-----------")
    val runDocker = (s"docker run --rm " +
      s"--network=$dockerName_net --ip=$ip " +
      s"--expose $masterPort --expose $slavePort --expose $hdfsPort " +
      s"-d --name $dockerName $dockerImage").!!
    info(runDocker)

    info("\n--------Docker run script startup-----------")
    val runStartup = s"docker exec $dockerName /opt/script/startup.sh".!!
    info(runStartup)
  }

  override def afterAll(): Unit ={
    info("AfterAll!")
    info("\n--------Closing FileSistem-----------")
    info(FileSystem.closeAll())

    info("\n--------Docker stop-----------")
    val stopDocker = s"docker stop $dockerName".!!
    info(stopDocker)

    info("\n--------rm network-----------")
    val rmNetwork = s"docker network rm $dockerName_net".!!
    info(rmNetwork)
  }
}
