/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
package org.neo4j.morpheus.templates

import java.io.File
import java.net.URI

import com.neo4j.cypher.spark.Neo4jNamedGraphSource
import com.neo4j.morpheus.api.GraphSources
import com.neo4j.sql.SqlGraphSource
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.neo4j.Neo4jConfig

object NorthwindMSDNTemplate extends App {

  // ========================================================
  // Initialization
  // ========================================================

  // parse program arguments

  val sparkMasterAddress: String = parseArgs(args)

  val hdfsRootFolder: String = parseArgs(args)
  val hdfsUser: String = parseArgs(args)

  val sqlDDLSchemaFile: String = parseArgs(args)
  val sqlSourceMappingsFile: String = parseArgs(args)

  val neo4jBoltURI: String = parseArgs(args)

  val rootDirectoryPath: java.nio.file.Path = new File(parseArgs(args)).toPath

  // initialize CAPS

  implicit val (caps: CAPSSession, fs: FileSystem) = initCAPS()

  // register graph data sources using domain specific namespaces
  private val sqlGraphSource = GraphSources
      .sql(rootDirectoryPath, sqlDDLSchemaFile)
      .withDataSourcesFile(sqlSourceMappingsFile)

  // (:Employee { employeeId })-[:REPORTS_TO]->(:Employee)
  caps.registerSource(Namespace("northwind"), sqlGraphSource)
  // (:User { userId, [employeeId])-[:REPLY_TO]->(:Question { content })
  caps.registerSource(Namespace("msdn"), GraphSources.cypher.namedGraph(Neo4jConfig(URI.create(neo4jBoltURI))))
  // (:Event { type, userId, timestamp })
  caps.registerSource(Namespace("msdn_logs"), GraphSources.fs(hdfsRootFolder).parquet())

  // ========================================================
  // Data Integration
  // ========================================================

  // Connect mid-level Northwind employees with MSDN users
  val northwindAndMSDN = caps.cypher(
    """
      |// Find mid-level employees from Northwind
      |FROM GRAPH northwind.regionUS
      |MATCH (boss:Employee)
      |WHERE NOT EXISTS (boss)-[:REPORTS_TO]->(:Employee)
      |MATCH (emp:Employee)-[:REPORTS_TO*2..4]->(boss)
      |WITH DISTINCT emp
      |
      |// Load MSDN users with at least 10 replies
      |FROM GRAPH msdn.2018-04-05
      |MATCH (usr:User)-[r:REPLY_TO]->(:Question)
      |WHERE count(r) > 10
      |
      |// Join Employees with Users
      |AND emp.employeeId = usr.employeeId
      |
      |// Integrate both networks and create a link between Employees and Users
      |CONSTRUCT
      |   // Take everything from the input graphs ...
      |   ON northwind.regionUS, msdn.2018-04-05
      |   // ... clone matching employees and users ...
      |   CLONE emp as clonedEmp, usr as clonedUsr
      |   // ... and create a new relationship between them.
      |   NEW (clonedEmp)-[:SAME_AS]->(clonedUsr)
      |
      |// return the integrated graph
      |RETURN GRAPH
    """.stripMargin).getGraph

  // store the resulting graph in the CAPS session for later access
  caps.catalog.store("northwindAndMSDN", northwindAndMSDN)

  // We combine the logs of interest (Q3 + Q4 2017), connect them to MSDN users and count the number of logins within
  // working hours. We create a new node storing those event statistics and connect it to the user.
  val warehouseGraph = caps.cypher(
    """
      |// Use Cypher to union all website logs from Q3 and Q4
      |CREATE GRAPH session.logsOfInterest {
      |   FROM GRAPH msdn_logs.2017.Q3
      |   RETURN GRAPH
      |   UNION ALL
      |   FROM GRAPH msdn_logs.2017.Q4
      |   RETURN GRAPH
      |}
    """.stripMargin)
    .getGraph
    // Measure stats within that time and connect it to the users.
    .cypher(
      """
        |FROM GRAPH session.logsOfInterest
        |// Find login events that happened within working hours
        |MATCH (e:Event { type: 'login' })
        |WHERE hour_of_day(e.timestamp) > 9 AND hour_of_day(e.timestamp) < 17
        |
        |// connect the events with MSDN users
        |FROM session.northwindAndMSDN
        |MATCH (usr:User)
        |WHERE e.userId = usr.userId
        |
        |// Count the number of login events per User
        |WITH usr, count(e) AS loginCnt
        |
        |CONSTRUCT
        |   // Continue with the integrated graph and ...
        |   ON session.northwindAndMSDN
        |   // ... clone the user ...
        |   CLONE usr AS clonedUsr
        |   // ... and create a new node storing the event statistics for that user.
        |   NEW (clonedUsr CLONE OF usr)-[:HAS]->(:EventStats { logins = loginCnt })
        |RETURN GRAPH
      """.stripMargin).getGraph

  caps.catalog.store("warehouse", warehouseGraph)

  // Compute the top 10 candidates for promotions based on our integrated graph warehouse
  val promotionCandidates = caps.cypher(
    """
      |FROM session.warehouse
      |MATCH (emp:Employee)-[:IS]->(:User)-[:HAS]->(stats:EventStats)
      |WHERE stats.logins > 1000
      |RETURN emp.firstName, emp.lastName, stats.logins AS loginCnt
      |ORDER BY loginCnt DESC
      |LIMIT 10
    """.stripMargin)

  promotionCandidates.show

  // ========================================================
  // Utility functions
  // ========================================================

  def initCAPS(): (CAPSSession, FileSystem) = {

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
      URI.create(hdfsRootFolder),
      sparkSession.sparkContext.hadoopConfiguration,
      hdfsUser
    )
    capsSession -> fileSystem
  }

  def parseArgs(strings: Array[String]): String = ???

}
