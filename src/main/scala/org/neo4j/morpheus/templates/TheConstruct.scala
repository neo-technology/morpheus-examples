package org.neo4j.morpheus.templates

import com.neo4j.morpheus.api.GraphSources
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

/**
  * "This is the Construct. It's our loading program.
  * We can load anything... From clothing to equipment,
  * weapons, training simulations; anything we need."
  *
  * -- Morpheus, on the Construct
  */
object TheConstruct extends App {

  implicit val session: CAPSSession = CAPSSession.local()

  session.registerSource(Namespace("myLocal"), GraphSources.fs("file:///my/local/file/path").parquet())

  session.registerSource(Namespace("datalake"), GraphSources.fs("hdfs:///my/remote/hdfs/path").parquet())

  session.cypher("CREATE GRAPH myLocal.graph1 { CONSTRUCT NEW (:A {foo: 1}) RETURN GRAPH }")

}
