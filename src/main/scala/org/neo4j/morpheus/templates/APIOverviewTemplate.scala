/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
package org.neo4j.morpheus.templates

import java.io.File
import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.neo4j.cypher.spark.EnterpriseNeo4jGraphSource
import org.neo4j.morpheus.api.MorpheusGraphSource
import org.opencypher.okapi.api.graph.{Namespace, PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.neo4j.Neo4jConfig

object APIOverviewTemplate extends App {

  /*
   * Arguments to this program:
   *  - Spark master address
   *  - URI to WKDS root in HDFS
   *  - HDFS user name
   *  - Path to file containing SQL DDL
   *  - Path to file containing SQL graph mappings
   *  - Bolt connection URI to a Neo4j database
   *  - Root directory path location to the sql datasources file
   */
  override def main(args: Array[String]): Unit = {

    val sparkMasterAddress = parseArgs(args)
    val hdfsURItoWkds = parseArgs(args)
    val hdfsUser = parseArgs(args)
    val sqlDDLFilename = parseArgs(args)
    val sqlSourceMappingsFilename = parseArgs(args)
    val boltURI = parseArgs(args)
    val rootDirectoryPath: java.nio.file.Path = new File(parseArgs(args)).toPath

    // init your Spark session
    val conf = new SparkConf()
    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName("master-example")
      .config(conf)
      .enableHiveSupport()
      .master(sparkMasterAddress)
      .getOrCreate()

    // init your CAPS session
    implicit val capsSession: CAPSSession = CAPSSession.create

    // init a connection to HDFS
    implicit val fileSystem: FileSystem = FileSystem.get(
      URI.create(hdfsURItoWkds),
      sparkSession.sparkContext.hadoopConfiguration,
      hdfsUser
    )

    // register PropertyGraph data sources
    // can register multiple session data sources
    capsSession.registerSource(Namespace("working"), new SessionGraphDataSource())
    // mount data source at an arbitrary namespace
    capsSession.registerSource(Namespace("datalake_europe"), MorpheusGraphSource.parquet(hdfsURItoWkds))
    capsSession.registerSource(Namespace("sql"), MorpheusGraphSource.sql(rootDirectoryPath, sqlDDLFilename, sqlSourceMappingsFilename))
    capsSession.registerSource(Namespace("neo4j"), new EnterpriseNeo4jGraphSource(Neo4jConfig(URI.create(boltURI))))

    // query across multiple graphs hosted in different environments
    // get a graph back containing the two input graphs, plus new relationships between nodes that seem to represent the same thing
    val connections = capsSession.cypher(
      """
        |FROM neo4j.graph
        |MATCH (p:Person)
        |FROM datalake_europe.customers
        |MATCH (c:Customer)
        |WHERE p.email = c.email
        |CONSTRUCT
        |  ON neo4j.graph, datalake_europe.customers
        |  CLONE p, c
        |  NEW (p)-[:SAME_EMAIL_AS]->(c)
        |RETURN GRAPH
      """.stripMargin).getGraph

    // continue querying on the newly constructed graph
    // get a table back with dog licence numbers and their owners' name and age
    val dogOwners = connections.cypher(
      """
        |MATCH (c:Customer)<-[:SAME_EMAIL_AS]-(p:Person)
        |FROM sql.census2018
        |MATCH (r:Resident)<--(dog:LicensedDog)
        |WHERE r.person_number = c.person_number
        |RETURN dog.licenceNumber AS dog, p.name AS ownerName, p.age AS ownerAge
      """.stripMargin).getRecords

    // register the dogOwner table as a Spark SQL view
    // this requires dropping out of public API and escaping into the underlying Spark API
    import org.opencypher.spark.impl.CAPSConverters._
    dogOwners.asCaps.toDF().createOrReplaceTempView("dogOwners")

    // query the registered table using SQL
    // get back a table of dogs owned by older people and their owners' name
    val sqlResult = capsSession.sql("SELECT dog, ownerName AS owner FROM dogOwners WHERE ownerAge > 60")

    // use the SQL result table as input driving table to a Cypher query
    // create a new graph in the HDFS WKDS; this is a side-effect query with no result value
    capsSession.cypher(
      """
        |CREATE GRAPH datalake-europe.tuesday.dogOwners {
        |  WITH dog AS dog, owner AS owner  // illustrates the input
        |  CONSTRUCT
        |    NEW (:Dog {licenseNumber: dog})-[:OWNED_BY]->(:Owner:Person {name: owner})
        |  RETURN GRAPH
        |}
      """.stripMargin, drivingTable = Some(sqlResult))

    // remove monday's copy
    capsSession.cypher("DELETE GRAPH datalake_europe.monday.dogOwners")

    // pick the graph up from HDFS again
    val graph: PropertyGraph = capsSession.catalog.graph(QualifiedGraphName("datalake-europe.tuesday.dogOwners"))

    // register a graph in the session data source (volatile memory)
    // the default namespace for the session data source is 'session'
    // the graphs session.dogOwners and hdfs2.dogOwners are now exactly the same graph
    capsSession.catalog.store("dogOwners", graph)

    // query the session-stored dog owners graph to find 100 people with the most owned dogs
    val top100DogOwners = capsSession.cypher(
      """
        |FROM session.dogOwners
        |MATCH (d:Dog)-[:OWNED_BY]->(o:Owner)
        |WITH o.name, count(d) AS nbrDogs
        |RETURN o.name AS owner, nbrDogs
        |  ORDER BY nbrDogs DESC
        |  LIMIT 100
      """.stripMargin)

    // =========

    // register in the session catalog
    capsSession.catalog.store("connections", connections)

    // connect with an ERP graph to find interesting purchases
    // find friends of dog owners that have bought dog and pet toy products
    // recommend these pet toys to the dog owners
    // this constructs a new graph of recommendations, then continues to match on that graph directly
    // the recommendation graph only exists in the context of the query
    val recommendations = capsSession.cypher(
      """
        |FROM datalake_europe.transactions
        |MATCH (c:Customer)-[:BOUGHT]->(:Product {category: 'dogs'}),
        |      (c)-[:BOUGHT]->(product:Product {category: 'pet-toys'})
        |FROM session.connections
        |MATCH (:Customer {email: c.email})-[:SAME_EMAIL_AS]->(person:Person),
        |      (person)-[:KNOWS]->(dogOwner:Person)
        |FROM session.dogOwners
        |MATCH (:Owner {name: dogOwner.name})
        |CONSTRUCT
        |  ON connections, datalake_europe.transactions
        |  CLONE product, friend, person
        |  NEW (friend)-[:SHOULD_BUY]->(product)-[:BOUGHT_BY]->(person)
        |MATCH (friend:Person)-[:SHOULD_BUY]->(product:Product)-[:BOUGHT_BY]->(person:Person)
        |RETURN friend.name AS name, product.name AS productName, person.name AS reference
        |  ORDER BY product.price DESC
      """.stripMargin)

    // print results to console
    recommendations.show
  }

  private def parseArgs(strings: Array[String]): String = ???

}
