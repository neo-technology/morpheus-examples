package org.neo4j.morpheus.examples

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.neo4j.hdfs.parquet.HdfsParquetGraphSource
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}

import scala.collection.JavaConverters._

object ParquetWriteExample extends App {

  // Create CAPS session, retrieve Spark session and register a Parquet Graph Source
  implicit val session: CAPSSession = CAPSSession.local()
  val spark = session.sparkSession
  // // Register the Parquet data source within the CAPS session and point it to the local filesystem, i.e. <project-dir>/target/classes/.
  session.registerSource(Namespace("myParquet"), HdfsParquetGraphSource(new Path(getClass.getResource("/").getPath)))

  // Generate some DataFrames that we'd like to interpret as a property graph.
  val nodesDF = SocialNetworkDataFrames.nodes(spark)
  val relsDF = SocialNetworkDataFrames.rels(spark)

  // Generate node- and relationship tables that wrap the DataFrames. The mapping between graph entities and columns
  // is derived using naming conventions for identifier columns.
  val personTable = CAPSNodeTable(Set("Person"), nodesDF)
  val friendsTable = CAPSRelationshipTable("KNOWS", relsDF)

  // Create property graph from graph scans
  val graph = session.readFrom(personTable, friendsTable)

  // Store the graph using the Parquet Graph Source (can be found in <project-dir>/target/classes/socialNetwork)
  session.catalog.store("myParquet.socialNetwork", graph)
}

object SocialNetworkDataFrames {
  def nodes(session: SparkSession): DataFrame = {
    val nodes = List(
      Row(0L, "Alice", 42L),
      Row(1L, "Bob", 23L),
      Row(2L, "Eve", 84L)
    ).asJava
    val nodeSchema = StructType(List(
      StructField("id", LongType, false),
      StructField("name", StringType, false),
      StructField("age", LongType, false))
    )
    session.createDataFrame(nodes, nodeSchema)
  }

  def rels(session: SparkSession): DataFrame = {
    val rels = List(
      Row(0L, 0L, 1L, "23/01/1987"),
      Row(1L, 1L, 2L, "12/12/2009")
    ).asJava
    val relSchema = StructType(List(
      StructField("id", LongType, false),
      StructField("source", LongType, false),
      StructField("target", LongType, false),
      StructField("since", StringType, false))
    )
    session.createDataFrame(rels, relSchema)
  }
}
