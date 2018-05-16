package org.neo4j.morpheus.refactor

import org.apache.spark.sql.{Column, DataFrame}
import org.neo4j.cypher.spark.udfs.CapUdfs.caps_monotonically_increasing_id
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}

/**
  * Utility class to encapsulate repeated dataframe manipulation operations.
  * Primarily now this refactors one->many relationships inside of a single relation
  * out into nodes and relationships.  Common pattern in source data is to have this:
  * Accident,Location
  * 1,5
  * 2,5,
  * 3,6
  *
  * Here there are many accidents in a single location.  We want to refactor out a location
  * table, with rels Accident -> Location.  In this example Location is a "Nested Node" in
  * the accident table.
  */
object Refactor {
  val curId: Column = caps_monotonically_increasing_id(0)
  val reservedIdField = "__refactored_id"

  def refactor(d: DataFrame, node: NestedNode): GraphPart = {
    val refactoredNodeDF = d.select(node.nestedKey, node.extraProps:_*)
      .distinct()
      .withColumn(reservedIdField, curId)
      .capsWorkaround()

    val bits = d.select(node.pk, node.nestedKey)
      .withColumnRenamed(node.nestedKey, node.nestedKey + "_tmp")

    val refactoredRelsDF = bits
      .join(refactoredNodeDF, refactoredNodeDF.col(node.nestedKey) === bits.col(node.nestedKey + "_tmp"))
      .withColumnRenamed(node.pk, "from")
      .withColumnRenamed(reservedIdField, "to")
      .withColumn(reservedIdField, curId)
      .drop(node.nestedKey + "_tmp", node.nestedKey)
      .capsWorkaround()

    // println("NODES AND SCHEMA")
    // refactoredNodeDF.show(5)
    // println(refactoredNodeDF.schema.prettyJson)

    // println("RELS AND SCHEMA")
    // refactoredRelsDF.show(5)
    // println(refactoredRelsDF.schema.prettyJson)

    val nodeMapping = NodeMapping
      .withSourceIdKey(reservedIdField)
      .withImpliedLabel(node.nestedLabel)
      .withPropertyKey(node.nestedKey)
      .withPropertyKeys(node.extraProps:_*)

    val relMapping = RelationshipMapping
      .withSourceIdKey(reservedIdField)
      .withSourceStartNodeKey("from")
      .withSourceEndNodeKey("to")
      .withRelType(node.relType)

    GraphPart(
      CAPSNodeTable(nodeMapping, refactoredNodeDF),
      Some(CAPSRelationshipTable(relMapping, refactoredRelsDF))
    )
  }

  implicit class CAPSWorkarounds(df: DataFrame) {
    // Per Martin, id must be in the first column position, bugfix underway.
    def capsWorkaround(): DataFrame = {
      val sortedCols = df.columns.sortWith((l, r) => l == reservedIdField)
      df.select(sortedCols.head, sortedCols.tail: _*)
    }
  }
}
