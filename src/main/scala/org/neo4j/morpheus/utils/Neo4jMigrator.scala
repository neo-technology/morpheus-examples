package org.neo4j.morpheus.utils

import java.net.URI

import org.neo4j.cypher.spark.EnterpriseNeo4jGraphSource
import org.neo4j.morpheus.api.MorpheusGraphSource
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.neo4j.Neo4jConfig

object Neo4jMigrationWriter extends App {

  implicit val session = CAPSSession.local()

  session.registerSource(Namespace("file"), MorpheusGraphSource.parquet(getClass.getResource("/parquet/").getPath))
  session.registerSource(Namespace("neo4j"), new EnterpriseNeo4jGraphSource(Neo4jConfig(URI.create("bolt://localhost:7687"), "neo4j", Some("passwd"))))

  val FILE_ROOT = ""

  session.cypher(
    """
      |CREATE GRAPH neo4j.sn {
      |  FROM GRAPH file.sn
      |  RETURN GRAPH
      |}
    """.stripMargin)

}

object Neo4jMigrationReader extends App {

  implicit val session = CAPSSession.local()

  session.registerSource(Namespace("file"), MorpheusGraphSource.orc("/tmp/graphs/orc"))
  session.registerSource(Namespace("neo4j"), new EnterpriseNeo4jGraphSource(Neo4jConfig(URI.create("bolt://localhost:7687"), "neo4j", Some("passwd"))))

  val FILE_ROOT = ""

  session.cypher(
    """
      |CREATE GRAPH file.sn {
      |  FROM GRAPH neo4j.sn
      |  RETURN GRAPH
      |}
    """.stripMargin)

}