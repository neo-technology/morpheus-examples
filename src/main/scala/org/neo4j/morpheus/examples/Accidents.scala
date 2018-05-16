package org.neo4j.morpheus.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import org.neo4j.morpheus.refactor.NestedNode
import org.neo4j.morpheus.refactor.Refactor
import org.neo4j.cypher.spark.udfs.CapUdfs.caps_monotonically_increasing_id
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.UUID

import org.apache.spark.sql.functions.lit
import org.neo4j.morpheus.refactor

/**
  * Demonstrates basic usage of the CAPS API by loading an example network from existing [[DataFrame]]s and
  * running a Cypher query on it.
  */
object Accidents extends App {
  override def main(args: Array[String]) {
    println("My args were " + args.mkString(", "))
    val csvFile = args(0)

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()

    // 1) Create CAPS session and retrieve Spark session
    implicit val session: CAPSSession = CAPSSession.create(spark)
    // val spark = session.sparkSession

    val curId = caps_monotonically_increasing_id(0)

    val pk = "__id"

    val raw = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load(csvFile)

    val df = raw
      .withColumn(pk, curId)

    println("Beginning raw input data, tagged with IDs")
    df.show(5)

    // STEP 1 -- pull out just Accident nodes.
    // Workaround 1: Get 'id' column in first position
    val sortedCols = df.columns.sortWith((l, r) => l == "__id" )
    val accidentTable = df
      .select(sortedCols.head, sortedCols.tail: _*)

    val accident = CAPSNodeTable(
      NodeMapping.withSourceIdKey("__id")
        .withImpliedLabel("Accident")
        .withPropertyKeys("Location_Easting_OSGR", "Location_Northing_OSGR",
          "Longitude", "Latitude", "Police_Force", "Number_Of_Vehicles", "Number_Of_Casualties",
          "Date", "Time", "Urban_or_Rural_Area", "Did_Police_Officer_Attend_Scene_of_Accident",
          "LSOA_of_Accident_Location", "Accident_Index"),
      accidentTable.select("__id",
        "Location_Easting_OSGR", "Location_Northing_OSGR",
        "Longitude", "Latitude", "Police_Force", "Number_Of_Vehicles", "Number_Of_Casualties",
        "Date", "Time", "Urban_or_Rural_Area", "Did_Police_Officer_Attend_Scene_of_Accident",
        "LSOA_of_Accident_Location", "Accident_Index"
      ))

    // Road designation is a more complex structure in here we need to pre-process before
    // factoring out.
    val roads = df.select(pk, "1st_Road_Number", "1st_Road_Class", "Road_Type")
      .withColumnRenamed("1st_Road_Number", "Road_Number")
      .withColumnRenamed("1st_Road_Class", "Road_Class")
      .union(df.select(pk, "2nd_Road_Number", "2nd_Road_Class")
        .withColumnRenamed("2nd_Road_Number", "Road_Number")
        .withColumnRenamed("2nd_Road_Class", "Road_Class")
        // Dataset doesn't specify second road type, only first.
        .withColumn("Road_Type", lit("Unknown")))
      .distinct()
    val extraRoadNodeProps = Seq("Road_Type", "Road_Class")

    case class RefactorPair(df: DataFrame, nn: NestedNode)

    val thingsToRefactor = Seq(
      // Extract the "Accident_Severity" column as a property of a new node labeled
      // "Severity", so we end up with (:Accident)-[:SEVERITY]->(:Severity)
      RefactorPair(accidentTable, NestedNode(pk, "Severity", "Accident_Severity", "SEVERITY")),
      RefactorPair(accidentTable, NestedNode(pk, "LightCondition", "Light_Conditions", "CONDITION")),
      RefactorPair(accidentTable, NestedNode(pk, "WeatherCondition", "Weather_Conditions", "CONDITION")),
      RefactorPair(accidentTable, NestedNode(pk, "Year", "Year", "OCCURRED")),
      RefactorPair(accidentTable, NestedNode(pk, "DayOfWeek", "Day_Of_Week", "OCCURRED")),
      RefactorPair(accidentTable, NestedNode(pk, "RoadSurfaceCondition", "Road_Surface_Conditions", "CONDITION")),

      RefactorPair(roads, NestedNode(pk, "Road", "Road_Number", "LOCATION", extraRoadNodeProps:_*)),

      RefactorPair(accidentTable, NestedNode(pk, "PedCrossingFacility", "Pedestrian_Crossing-Physical_Facilities", "FACILITY")),
      RefactorPair(accidentTable, NestedNode(pk, "PedCrossingHumanControl", "Pedestrian_Crossing-Human_Control", "CONTROL")),
      RefactorPair(accidentTable, NestedNode(pk, "SpecialCondition", "Special_Conditions_at_Site", "CONDITION")),
      RefactorPair(accidentTable, NestedNode(pk, "Hazard", "Carriageway_Hazards", "HAZARD")),
      RefactorPair(accidentTable, NestedNode(pk, "District", "Local_Authority_(District)", "DISTRICT"))
    )

    val parts = thingsToRefactor
      .map(rfPair => Refactor.refactor(rfPair.df, rfPair.nn))

    val ns: Seq[CAPSNodeTable] = parts.map(p => p.nodes)
    val es: Seq[CAPSRelationshipTable] = parts.map(_.edges).filter(_.isDefined).map(_.get)
    val entityTables: Seq[CAPSEntityTable] = ns ++ es

    println("Assembling graph")
    val graph = session.readFrom(accident, entityTables:_*)

    println(graph.schema.pretty)

    println("What's the average severity for each day?")
    graph.cypher(
      """
        |MATCH (a:Accident)-[r:SEVERITY]->(s:Severity),
        |(a)-[:OCCURRED]->(d:DayOfWeek)
        |RETURN distinct(d.Day_Of_Week) as DayOfWeek,
        |  avg(toFloat(s.Accident_Severity)) AS avgSeverity
        |ORDER BY DayOfWeek ASC
      """.stripMargin
    ).getRecords.show

    println("Where are these happening")
    graph.cypher(
      """
        |MATCH (a:Accident)-[:LOCATION]->(r:Road)
        |WHERE r.Road_Number IS NOT NULL
        |RETURN distinct(r.Road_Number), r.Road_Type, r.Road_Class, count(a) as x
        |ORDER BY x desc
        |LIMIT 20
      """.stripMargin
    ).getRecords.show
  }
}
