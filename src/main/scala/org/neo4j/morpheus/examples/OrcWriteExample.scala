// tag::full-example[]
package org.neo4j.morpheus.examples

import com.neo4j.morpheus.api.GraphSources
import org.neo4j.morpheus.utils.{ConsoleApp, SocialNetworkDataFrames}
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}

object OrcWriteExample extends ConsoleApp {

  // Create CAPS session, retrieve Spark session and register a Parquet Graph Source
  implicit val session: CAPSSession = CAPSSession.local()
  val spark = session.sparkSession

  // Register the Orc data source within the CAPS session and point it to the local filesystem, i.e. <project-dir>/target/classes/.
  val orcGraphSource = GraphSources.fs(getClass.getResource("/").getPath).orc()
  session.registerSource(Namespace("myOrc"), orcGraphSource)

  // Generate some DataFrames that we'd like to interpret as a property graph.
  val nodesDF = SocialNetworkDataFrames.nodes(spark)
  val relsDF = SocialNetworkDataFrames.rels(spark)

  // Generate node- and relationship tables that wrap the DataFrames. The mapping between graph entities and columns
  // is derived using naming conventions for identifier columns.
  val personTable = CAPSNodeTable(Set("Person"), nodesDF)
  val friendsTable = CAPSRelationshipTable("KNOWS", relsDF)

  // Create property graph from graph scans
  val graph = session.readFrom(personTable, friendsTable)

  // Delete any graph existing under the selected name (from last run)
  session.catalog.delete("myOrc.socialNetwork")
  // Store the graph using the Orc Graph Source (can be found in <project-dir>/target/classes/socialNetwork)
  session.catalog.store("myOrc.socialNetwork", graph)
}
// end::full-example[]
