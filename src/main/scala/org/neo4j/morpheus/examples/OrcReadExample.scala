package org.neo4j.morpheus.examples

import org.apache.hadoop.fs.Path
import org.neo4j.hdfs.orc.HdfsOrcGraphSource
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

object OrcReadExample extends App {

  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Register the Orc data source within the CAPS session and point it to the local filesystem, i.e. <project-dir>/target/classes/orc.
  session.registerSource(Namespace("myOrc"), HdfsOrcGraphSource(new Path(getClass.getResource("/orc").getPath)))

  // Query the 'sn' graph stored within the Orc graph source
  val res = session.cypher(
    s"""
       |FROM GRAPH myOrc.sn
       |MATCH (n)-[r]->(m)
       |RETURN n, r, m
     """.stripMargin)

  // Show the query results
  res.show
}
