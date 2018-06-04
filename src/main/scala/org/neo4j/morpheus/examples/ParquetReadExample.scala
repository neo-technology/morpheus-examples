package org.neo4j.morpheus.examples

import org.neo4j.morpheus.api.MorpheusGraphSource
import org.neo4j.morpheus.utils.ConsoleApp
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

object ParquetReadExample extends ConsoleApp {

  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Register the Parquet data source within the CAPS session and point it to the local filesystem, i.e. <project-dir>/target/classes/parquet.
  session.registerSource(Namespace("myParquet"), MorpheusGraphSource.parquet(getClass.getResource("/parquet").getPath))

  // Query the 'sn' graph stored within the Parquet graph source
  val res = session.cypher(
    s"""
       |FROM GRAPH myParquet.sn
       |MATCH (n)-[r]->(m)
       |RETURN n, r, m
     """.stripMargin)

  // Show the query results
  res.show
}
