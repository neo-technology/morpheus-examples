// tag::full-example[]
package org.neo4j.morpheus.examples

import java.nio.file.Paths

import com.neo4j.morpheus.api.GraphSources
import org.apache.spark.sql.SparkSession
import org.neo4j.morpheus.utils.{CensusDB, ConsoleApp}
import com.neo4j.sql.SqlGraphSource
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

object JdbcSqlGraphSourceExample extends ConsoleApp {

  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Create the data in H2 in-memory database
  implicit val sparkSession: SparkSession = session.sparkSession
  val schema = "CENSUS"
  val driver = "org.h2.Driver"
  val jdbcUrl = s"jdbc:h2:mem:$schema.db;INIT=CREATE SCHEMA IF NOT EXISTS $schema;DB_CLOSE_DELAY=30;"
  CensusDB.createJdbcData(driver, jdbcUrl, schema)

  // Register a SQL source (for JDBC) in the Cypher session
  val graphName = "Census_1901"
  val sqlGraphSource = GraphSources
    .sql(
      Paths.get(getClass.getResource("/ddl").toURI),
      "censusGraph.sql"
    ).withDataSourcesFile("jdbc-data-sources.json")

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
}
// end::full-example[]
