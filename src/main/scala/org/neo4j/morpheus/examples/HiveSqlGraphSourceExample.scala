/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
// tag::full-example[]
package org.neo4j.morpheus.examples

import java.io.File
import java.nio.file.Paths

import com.neo4j.morpheus.api.GraphSources
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.neo4j.morpheus.utils.{CensusDB, ConsoleApp}
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

object HiveSqlGraphSourceExample extends ConsoleApp {

  // tag::create-session[]
  // Create CAPS session
  val settings = hiveExampleSettings
  implicit val session: CAPSSession = CAPSSession.local(settings: _*)
  // end::create-session[]

  // tag::prepare-sql-database[]
  // Create the data in H2 in-memory database
  implicit val sparkSession: SparkSession = session.sparkSession
  val schema = "CENSUS"
  CensusDB.createHiveData(schema)
  // end::prepare-sql-database[]

  // tag::register-sql-source-in-session[]
  // Register a SQL source (for Hive) in the Cypher session
  val graphName = "Census_1901"
  val sqlGraphSource = GraphSources
      .sql(Paths.get(getClass.getResource("/ddl").toURI), "censusGraph.sql")
      .withDataSourcesFile("hive-data-sources.json")
  session.registerSource(Namespace("sql"), sqlGraphSource)
  // end::register-sql-source-in-session[]

  // tag::access-registered-graph[]
  // Access the graph via its qualified graph name
  val census = session.catalog.graph("sql." + graphName)
  // end::access-registered-graph[]

  // tag::query-graph[]
  // Run a simple Cypher query
  census.cypher(
    s"""
       |FROM GRAPH sql.$graphName
       |MATCH (n:Person)-[r]->(m)
       |WHERE n.age >= 30
       |RETURN n,r,m
    """.stripMargin)
    .getRecords
    .show
  // end::query-graph[]

  // Set up temporary spark and hive directories for this example
  private def hiveExampleSettings: Seq[(String, String)] = {
    val sparkWarehouseDir = new File(s"spark-warehouse_${System.currentTimeMillis}").getAbsolutePath
    Seq(
      // ------------------------------------------------------------------------
      // Create a new unique local spark warehouse dir for every run - idempotent
      // ------------------------------------------------------------------------
      ("spark.sql.warehouse.dir", sparkWarehouseDir),
      // -----------------------------------------------------------------------------------------------------------
      // Create an in-memory Hive Metastore (only Derby supported for this mode)
      // This is to avoid creating a local HIVE "metastore_db" on disk which needs to be cleaned up before each run,
      // e.g. avoids database and table already exists exceptions on re-runs - not to be used for production.
      // -----------------------------------------------------------------------------------------------------------
      ("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:hms;create=true"),
      ("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver"),
      // ------------------------------------------------------------------------------------------------------------
      // An alternative way of enabling Spark Hive Support (e.g. you could use enableHiveSupport on the SparkSession)
      // ------------------------------------------------------------------------------------------------------------
      ("hive.metastore.warehouse.dir", s"warehouse_${System.currentTimeMillis}"),
      (CATALOG_IMPLEMENTATION.key, "hive") // Enable hive
    )
  }

}
// end::full-example[]
