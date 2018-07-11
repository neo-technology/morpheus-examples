package org.neo4j.morpheus.examples

import com.neo4j.morpheus.api.GraphSources
import org.neo4j.morpheus.utils.ConsoleApp
import org.neo4j.morpheus.utils.Neo4jHarnessUtils._
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

object Neo4jWriteExample extends ConsoleApp {

  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Start a Neo4j instance and populate it with social network data
  val neo4j = startNeo4j(personNetwork)

  // Register Graph Data Sources (GDS)
  session.registerSource(Namespace("socialNetwork"), GraphSources.cypher.neo4j(neo4j.dataSourceConfig))

  // Access the graph via its qualified graph name
  val socialNetwork = session.catalog.graph("socialNetwork.graph")

  // Run a simple Cypher query to find possible friends and create new relationships
  socialNetwork.cypher(
    s"""
       |CREATE GRAPH socialNetwork.recommendations {
       |  MATCH (n:Person)-[:FRIEND_OF]->(:Person)-[:FRIEND_OF]->(m:Person)
       |  WHERE NOT (n)-[:FRIEND_OF]-(m)
       |  CONSTRUCT
       |    NEW (n)-[:POSSIBLE_FRIEND]->(m)
       |  RETURN GRAPH
       |}
     """.stripMargin)

  // Print the results

  val res = session.cypher(
    s"""
       |FROM GRAPH socialNetwork.recommendations
       |MATCH (n)-[r:POSSIBLE_FRIEND]->(m)
       |RETURN n.name, m.name, COUNT(r) AS confidence
       |ORDER BY confidence DESC
     """.stripMargin)

  res.show

  // Shutdown Neo4j test instance
  neo4j.close()

  // TODO: remove explicit graph labels after https://github.com/neo-technology/morpheus/issues/75 is solved
  def personNetwork =
    s"""|CREATE (a:Person { name: 'Alice', age: 23 })
        |CREATE (b:Person { name: 'Bob', age: 42})
        |CREATE (c:Person { name: 'Carol', age: 1984})
        |CREATE (a)-[:FRIEND_OF { since: '23/01/1987' }]->(b)
        |CREATE (b)-[:FRIEND_OF { since: '12/12/2009' }]->(c)""".stripMargin

}
