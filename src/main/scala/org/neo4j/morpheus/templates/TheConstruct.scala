package org.neo4j.morpheus.templates

import org.neo4j.morpheus.api.MorpheusGraphSource
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

  session.registerSource(Namespace("myLocal"), MorpheusGraphSource.parquet("file:///my/local/file/path"))

  session.registerSource(Namespace("datalake"), MorpheusGraphSource.parquet("hdfs:///my/remote/hdfs/path"))

  session.cypher("CREATE GRAPH myLocal.graph1 { CONSTRUCT NEW (:A {foo: 1}) RETURN GRAPH }")

}
