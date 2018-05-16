package org.neo4j.morpheus.refactor

import org.apache.spark.sql.SparkSession
import org.neo4j.morpheus.examples.accidents.Graphable
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.CAPSNodeTable

class CodeTable(csvFile: String, codeField:String, descriptionField: String)(implicit spark:SparkSession, session: CAPSSession) extends Graphable {
  val df = capsWorkaround(spark.read
    .format("csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .load(csvFile)
    // Get rid of anything else in there except the mapping lookup
    .select(codeField, descriptionField)
    .withColumn(reservedId, curId))

  val defaultProps = Seq(codeField, descriptionField)

  def asGraph(label:String): PropertyGraph = {
    val mapping = NodeMapping.withSourceIdKey(reservedId)
      .withImpliedLabel(label)
      .withPropertyKeys(defaultProps:_*)

    session.readFrom(CAPSNodeTable(mapping, df))
  }
}
