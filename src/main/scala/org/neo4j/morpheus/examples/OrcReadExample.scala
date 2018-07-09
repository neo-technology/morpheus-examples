// tag::full-example[]
package org.neo4j.morpheus.examples

import com.neo4j.morpheus.api.GraphSources
import org.neo4j.morpheus.utils.ConsoleApp
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

object OrcReadExample extends ConsoleApp {

  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Register the Orc data source within the CAPS session and point it to the local filesystem, i.e. <project-dir>/target/classes/orc.
  val orcGraphSource = GraphSources.fs(getClass.getResource("/orc").getPath).orc()
  session.registerSource(Namespace("myOrc"), orcGraphSource)

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
// end::full-example[]
