package org.neo4j.morpheus.refactor

import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}

/**
  * Data carrier for a partial graph result
  */
case class GraphPart(nodes: CAPSNodeTable, edges: Option[CAPSRelationshipTable])
