package org.neo4j.morpheus.utils

import java.net.URI

import org.apache.hadoop.fs.Path
import org.neo4j.cypher.spark.EnterpriseNeo4jGraphSource
import org.neo4j.hdfs.parquet.HdfsParquetGraphSource
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.neo4j.Neo4jConfig

object Neo4jExporter extends App {

  implicit val session = CAPSSession.local()

  session.registerSource(Namespace("target"), new EnterpriseNeo4jGraphSource(Neo4jConfig(URI.create("bolt://localhost:7687"), "neo4j", Some("."))))

  session.registerSource(Namespace("source"), HdfsParquetGraphSource(root = new Path(getClass.getResource("/parquet").getFile)))

  session.cypher(
    """
      |CREATE GRAPH target.products {
      |  FROM GRAPH source.products
      |  RETURN GRAPH
      |}
    """.stripMargin)
}
