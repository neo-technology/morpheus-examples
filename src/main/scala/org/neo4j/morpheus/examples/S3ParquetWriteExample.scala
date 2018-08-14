/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
// tag::full-example[]
package org.neo4j.morpheus.examples

import com.neo4j.morpheus.api.GraphSources
import org.neo4j.morpheus.utils.{AWSHadoopSetup, ConsoleApp, SocialNetworkDataFrames}
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}

/**
  * Prior to running this example set the AWS environments variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
  * Then supply the s3 bucket via the "bucketName" variable below.
  */
object S3ParquetWriteExample extends ConsoleApp {
  // Create CAPS session, retrieve Spark session and register a Parquet Graph Source
  implicit val session: CAPSSession = CAPSSession.local()
  val spark = session.sparkSession

  // AWS Setup
  val bucketName = "morpheus-example-data"
  AWSHadoopSetup(bucketName)

  // Register the Parquet data source within the CAPS session and point it to the S3 filesystem,
  // Example: s3a://[bucketName]/morpheus-graphs/parquet
  val parquetGraphSource = GraphSources.fs("/morpheus-graphs/parquet").parquet()
  session.registerSource(Namespace("myParquet"), parquetGraphSource)

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
  session.catalog.delete("myParquet.socialNetwork")
  // Store the graph using the Parquet Graph Source (can be found in s3a://[bucketName]/morpheus-graphs/parquet/socialNetwork)
  session.catalog.store("myParquet.socialNetwork", graph)
}


// end::full-example[]
