package org.neo4j.morpheus.examples.accidents

import java.util.UUID

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.neo4j.cypher.spark.udfs.CapUdfs.caps_monotonically_increasing_id
import org.neo4j.morpheus.refactor.{NestedNode, Refactor, CodeTable}
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import scala.reflect.io.File

/**
  * Demonstrates basic usage of the CAPS API by loading an example network from existing [[DataFrame]]s and
  * running a Cypher query on it.
  */
object AccidentAnalysis extends App {
  override def main(args: Array[String]) {
    println("My args were " + args.mkString(", "))
    if (!File(args(0)).isDirectory) {
      println("Call me with a directory name that contains accident CSV files")
      return None
    }

    Logger.getLogger("org").setLevel(Level.WARN)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()

    // 1) Create CAPS session and retrieve Spark session
    implicit val session: CAPSSession = CAPSSession.create(spark)
    // val spark = session.sparkSession

    val csvDir = args(0)

    val codeGraphSources: Seq[(String, String)] = Seq(
      ("accident-severity.csv", "Severity"),
      ("journey-purpose.csv", "JourneyPurpose"),
      ("point-of-impact.csv", "PointOfImpact"),
      ("vehicle-propulsion.csv", "Propulsion"),
      ("junction-location.csv", "JunctionLocation"),
      ("casualty-type.csv", "CasualtyType")
    )

    // Store code lookup tables as their own mini-graphs.
    codeGraphSources
      .map((entry: (String, String)) => {
        val csvFile = entry._1
        val label = entry._2

        val graph = new CodeTable(csvDir + File.separator + csvFile, "code", "label")
          .asGraph(label)
        session.catalog.store(label, graph)
      })

    session.catalog.store("accident", new Accidents(csvDir).asGraph())
    session.catalog.store("vehicle", new Vehicles(csvDir).asGraph())
    session.catalog.store("casualties", new Casualties(csvDir).asGraph())

    println(session.catalog.graph("accident").schema.pretty)

    println("Multi-graph")
    session.cypher(
      """
        |FROM GRAPH accident
        |MATCH (a:Accident)-[r:SEVERITY]->(s:Severity)
        |FROM GRAPH Severity
        |MATCH (svExpanded:Severity)
        |WHERE s.Accident_Severity = svExpanded.code
        |CONSTRUCT ON accident, Severity
        | CLONE a, svExpanded
        | NEW (a)-[:SEVERITY]->(svExpanded)
        |RETURN a.Accident_Index, svExpanded.label
        |LIMIT 10
      """.stripMargin
    ).getRecords.show


    println("What's the average severity for each day?")
    session.catalog.graph("accident").cypher(
      """
        |MATCH (a:Accident)-[r:SEVERITY]->(s:Severity),
        |(a)-[:OCCURRED]->(d:DayOfWeek)
        |RETURN distinct(d.Day_Of_Week) as DayOfWeek,
        |  avg(toFloat(s.Accident_Severity)) AS avgSeverity
        |ORDER BY DayOfWeek ASC
      """.stripMargin
    ).getRecords.show

    println("Where are these happening")
    session.catalog.graph("accident").cypher(
      """
        |MATCH (a:Accident)-[:LOCATION]->(r:Road)
        |WHERE r.Road_Number IS NOT NULL
        |RETURN distinct(r.Road_Number) as Road_Number, r.Road_Type, r.Road_Class, count(a) as x
        |ORDER BY Road_Number desc
        |LIMIT 20
      """.stripMargin
    ).getRecords.show
  }
}
