package org.neo4j.morpheus.examples.accidents

import java.net.URI
import java.util.UUID

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.neo4j.cypher.spark.EnterpriseNeo4jGraphSource
import org.neo4j.morpheus.refactor.CodeTable
import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._

import scala.reflect.io.File

/**
  * Demonstrates basic usage of the CAPS API by loading an example network from existing [[DataFrame]]s and
  * running a Cypher query on it.
  */
object AccidentAnalysis extends App {
  override def main(args: Array[String]) {
    println("Call me with a directory name that contains accident CSV files")

    println("////////////////////////////////////////////////////")
    println("AccidentAnalysis " +  args + "//" + args(0))
    println("////////////////////////////////////////////////////")

    Logger.getLogger("org").setLevel(Level.WARN)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()

    // 1) Create CAPS session and retrieve Spark session
    implicit val session: CAPSSession = CAPSSession.create(spark)

    val csvDir = args(0)

    // All of these are code lookup tables with schema code,label
    val codeGraphSources: Seq[(String, String)] = Seq(
      ("accident-severity.csv", "Severity"),
      ("journey-purpose.csv", "JourneyPurpose"),
      ("point-of-impact.csv", "PointOfImpact"),
      ("vehicle-propulsion.csv", "Propulsion"),
      ("junction-location.csv", "JunctionLocation"),
      ("casualty-type.csv", "CasualtyType"),
      ("age-band.csv", "AgeGroup"),
      ("bus-passenger.csv", "BusPassenger"),
      ("car-passenger.csv", "CarPassenger"),
      ("casualty-class.csv", "CasualtyClass"),
      ("hit-object-in-carriageway.csv", "HitObjectInCarriageway"),
      ("hit-object-off-carriageway.csv", "HitObjectOffCarriageway"),
      ("home-area-type.csv", "HomeAreaType")
    )

    // Store code lookup tables as their own mini-graphs.
    codeGraphSources
      .map((entry: (String, String)) => {
        val csvFile = entry._1
        val label = entry._2

        val codeTbl = new CodeTable(csvDir + File.separator + csvFile, "code", "label")
        codeTbl.df.persist(StorageLevel.MEMORY_ONLY)

        val graph = codeTbl.asGraph(label).asCaps
        graph.cache()

        session.catalog.store(label, graph)
      })

    session.catalog.store("makemodel", new CarMakeModel(csvDir).asGraph())
    session.catalog.store("accident", new Accidents(csvDir).asGraph())
    session.catalog.store("vehicle", new Vehicles(csvDir).asGraph())
    session.catalog.store("casualties", new Casualties(csvDir).asGraph())

    println(session.catalog.graph("accident").schema.pretty)

    def queryHelper(q: String) = {
      session.cypher(q).getRecords.show
    }

    println("Multi-graph queries, Yay!")

    queryHelper(
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
    )

    println("Prove that aggregate casualty numbers match the cypher join")
    queryHelper(
      """
        |FROM GRAPH accident
        |MATCH (a:Accident)
        |FROM GRAPH casualties
        |MATCH (c:Casualty)
        |WHERE c.Accident_Index = a.Accident_Index
        |CONSTRUCT ON accident, casualties
        | CLONE a, c
        | NEW (a)-[:CASUALTY]->(c)
        |RETURN a.Accident_Index, a.Number_of_Casualties, count(c) as Matches
        |ORDER BY Matches DESC
        |LIMIT 10
      """.stripMargin
    )

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

    /*
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
    */

    /*
        Currently this fails due to this bug:
        https://trello.com/c/YtGiWfeQ/237-enterpriseneo4jpropertygraphsource-does-not-properly-escape-property-names-when-writing-to-neo4j

    val writeToNeo4j = true
    if (args.length >= 2 && writeToNeo4j) {
      println("Storing accident graph")
      val neo4j = EnterpriseNeo4jGraphSource(new URI(args(1)))
      neo4j.store(GraphName("accident"), session.catalog.graph("accident"))
    }
    */

    /* Works locally, fails on Cloudera cluster
    val megaQuery =
      """
        |FROM GRAPH accident
        | MATCH (accident:Accident)-[:SEVERITY]->(sev:Severity)
        |FROM GRAPH casualties
        | MATCH (casualty:Casualty)
        |FROM GRAPH vehicle
        | MATCH (vehicle:Vehicle)
        |FROM GRAPH makemodel
        | MATCH (vehDetails:MakeModel)
        |FROM GRAPH Severity
        | MATCH (svExpanded:Severity)
        |FROM GRAPH PointOfImpact
        | MATCH (pointOfImpact:PointOfImpact)
        |FROM GRAPH JourneyPurpose
        | MATCH (journeyPurpose:JourneyPurpose)
        |FROM GRAPH AgeGroup
        | MATCH (ageGroup:AgeGroup)
        |FROM GRAPH Propulsion
        | MATCH (propulsion:Propulsion)
        |WHERE
        |   accident.Accident_Index = vehDetails.Accident_Index AND
        |   accident.Accident_Index = casualty.Accident_Index AND
        |   accident.Accident_Index = vehicle.Accident_Index AND
        |   vehicle.Vehicle_Reference = vehDetails.Vehicle_Reference AND
        |   sev.Accident_Severity = svExpanded.code AND
        |   vehicle.`1st_Point_of_Impact` = pointOfImpact.code AND
        |   vehicle.Journey_Purpose_of_Driver = journeyPurpose.code AND
        |   vehicle.Age_Band_of_Driver = ageGroup.code AND
        |   vehicle.Propulsion_Code = propulsion.code
        |
        |CONSTRUCT ON
        |   accident, Severity, casualties,
        |   vehicle, makemodel, AgeGroup, Propulsion
        |
        | CLONE
        |   accident, svExpanded, vehicle,
        |   casualty, vehDetails, pointOfImpact,
        |   journeyPurpose, ageGroup, propulsion
        |
        | NEW (accident)-[:SEVERITY]->(svExpanded)
        | NEW (accident)-[:VEHICLE]->(vehicle)-[:DETAILS]->(vehDetails)
        | NEW (vehicle)-[:IMPACT]->(pointOfImpact)
        | NEW (vehicle)-[:PURPOSE]->(journeyPurpose)
        | NEW (vehicle)-[:DRIVER_AGE_GROUP]->(ageGroup)
        | NEW (vehicle)-[:PROPULSION]->(propulsion)
        | NEW (accident)-[:CASUALTY]->(casualty)
        |RETURN GRAPH
      """.stripMargin

    session.cypher(megaQuery).getRecords
    println("Integrating the graphs with this big query...")
    session.catalog.store("integratedGraph", session.cypher(megaQuery).getGraph)

    println("Querying resulting integrated graph")
    session.catalog.graph("integratedGraph").cypher(
      """
        |MATCH (accident:Accident)-[:SEVERITY]->(sev:Severity),
        |     (accident)-[:VEHICLE]->(veh:Vehicle)-[:IMPACT]->(pointOfImpact:PointOfImpact)
        |WHERE
        |   sev.label = 'Serious' or sev.label = 'Fatal' AND
        |   pointOfImpact.label is not null
        |RETURN
        |   sev.label as Severity,
        |   pointOfImpact.label as PointOfImpact,
        |   count(accident) as Accidents
        |ORDER BY Accidents DESC
        |LIMIT 20
      """.stripMargin
    ).getRecords.show
    */
  }
}
