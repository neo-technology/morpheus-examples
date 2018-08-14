/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
// tag::full-example[]
package org.neo4j.morpheus.examples

import com.neo4j.morpheus.api.GraphSources
import org.neo4j.morpheus.utils.{AWSHadoopSetup, ConsoleApp}
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

/**
  * Prior to running this example set the AWS environments variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
  * Then supply the s3 bucket via the "bucketName" variable below.
  */
object S3ParquetReadExample extends ConsoleApp {
  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // AWS Setup
  val bucketName = "morpheus-example-data"
  AWSHadoopSetup(bucketName)

  // Register the Parquet data source within the CAPS session and point it to the s3 filesystem.
  // Example: s3a://[bucketName]/morpheus-graphs/parquet
  val parquetGraphSource = GraphSources.fs("/morpheus-graphs/parquet").parquet()
  session.registerSource(Namespace("myParquet"), parquetGraphSource)

  // Query the 'sn' graph stored within the Parquet graph source
  val res = session.cypher(
    s"""
       |FROM GRAPH myParquet.socialNetwork
       |MATCH (n)-[r]->(m)
       |RETURN n, r, m
     """.stripMargin)

  // Show the query results
  res.show
}

// end::full-example[]
