package org.neo4j.morpheus.examples.accidents

import org.apache.spark.sql.DataFrame
import org.neo4j.cypher.spark.udfs.CapUdfs.caps_monotonically_increasing_id
import org.opencypher.okapi.api.graph.PropertyGraph

trait Graphable {
  val reservedId = "__reserved_id"
  val curId = caps_monotonically_increasing_id(0)

  def asGraph(label:String): PropertyGraph
  
  def capsWorkaround(df:DataFrame) = {
    val sortedCols = df.columns.sortWith((l, r) => l == reservedId)
    df.select(sortedCols.head, sortedCols.tail: _*)
  }
}