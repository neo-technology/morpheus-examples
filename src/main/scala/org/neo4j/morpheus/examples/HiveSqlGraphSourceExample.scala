package org.neo4j.morpheus.examples

import java.io.File
import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.neo4j.morpheus.converter.SqlDataExample
import org.neo4j.sql.SqlGraphSource
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

object HiveSqlGraphSourceExample extends App {
  // Create CAPS session
  val sparkWarehouseDir = new File(s"spark-warehouse_${System.currentTimeMillis}").getAbsolutePath
  val settings = hiveExampleSettings
  implicit val session: CAPSSession = CAPSSession.local(settings: _*)

  // Create the data in H2 in-memory database
  implicit val sparkSession: SparkSession = session.sparkSession
  val schema = "CENSUS"
  SqlDataExample.createHiveData(schema)

  // Register a SQL source (for Hive) in the Cypher session
  val graphName = "Census_1901"
  val sqlGraphSource = SqlGraphSource(
    rootDirectoryPath = Paths.get(getClass.getResource("/ddl").toURI),
    graphSchemaDDLFile = "censusGraph.sql",
    graphSQLDataSourcesFile = "hive-data-sources.json")
  session.registerSource(Namespace("sql"), sqlGraphSource)

  // Access the graph via its qualified graph name
  val census = session.catalog.graph("sql." + graphName)

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

  // Helper for this example
  private def hiveExampleSettings: Seq[(String, String)] = {
    Seq(
      // ---------------------------------------------------------------
      // Create a new unique local new spark warehouse dir for every run
      // ---------------------------------------------------------------
      ("spark.sql.warehouse.dir", sparkWarehouseDir),
      // ------------------------------------------------------------------------------------------------------
      // Create an in-memory Hive Metastore (only Derby supported for this mode)
      // This is to avoid database and table already exists errors for re-runs - not to be used for production.
      // ------------------------------------------------------------------------------------------------------
      ("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:hms;create=true"),
      ("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver"),
      // All alernative way of enabling Spark Hive Support (e.g.  enableHiveSupport
      ("hive.metastore.warehouse.dir", s"warehouse_${System.currentTimeMillis}"),
      (CATALOG_IMPLEMENTATION.key, "hive") // Enable hive
    )
  }

}
