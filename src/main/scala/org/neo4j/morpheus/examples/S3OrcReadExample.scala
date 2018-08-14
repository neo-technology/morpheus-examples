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
object S3OrcReadExample extends ConsoleApp {
  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // AWS Setup
  val bucketName = "morpheus-example-data"
  AWSHadoopSetup(bucketName)

  // Register the Orc data source within the CAPS session and point it to the S3 filesystem
  // Example: s3a://[bucketName]/morpheus-graphs/orc
  val orcGraphSource = GraphSources.fs("/morpheus-graphs/orc").orc()
  session.registerSource(Namespace("myOrc"), orcGraphSource)

  // Query the 'sn' graph stored within the Orc graph source
  val res = session.cypher(
    s"""
       |FROM GRAPH myOrc.socialNetwork
       |MATCH (n)-[r]->(m)
       |RETURN n, r, m
     """.stripMargin)

  // Show the query results
  res.show

  AWSHadoopSetup.tearDown
}

// end::full-example[]
