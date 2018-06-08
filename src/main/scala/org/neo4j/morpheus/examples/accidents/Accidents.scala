package org.neo4j.morpheus.examples.accidents

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel
import org.neo4j.morpheus.refactor.{NestedNode, Refactor}
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable, EntityTable}
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._

import scala.reflect.io.File

class Accidents(csvDir:String)(implicit spark:SparkSession, session: CAPSSession) extends Graphable {
  val pk = "__id"

  val fields = Seq(
    "Location_Easting_OSGR", "Location_Northing_OSGR",
    "Longitude", "Latitude", "Police_Force", "Number_Of_Vehicles", "Number_of_Casualties",
    "Date", "Time", "Urban_or_Rural_Area", "Did_Police_Officer_Attend_Scene_of_Accident",
    "LSOA_of_Accident_Location", "Accident_Index")

  val raw = spark.read
    .format("csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .load(csvDir + File.separator + "dftRoadSafety_Accidents_2016.csv")

  val df = raw
    .withColumn(pk, curId)

  def asGraph(label:String = "Accident") : PropertyGraph = {
    df.show(5)

    // STEP 1 -- pull out just Accident nodes.
    // Workaround 1: Get 'id' column in first position
    val sortedCols = df.columns.sortWith((l, r) => l == "__id" )
    val accidentTable = df
      .select(sortedCols.head, sortedCols.tail: _*)

    accidentTable.persist(StorageLevel.MEMORY_AND_DISK)

    val accident = CAPSNodeTable.fromMapping(
      NodeMapping.withSourceIdKey("__id")
        .withImpliedLabel(label)
        .withPropertyKeys(fields:_*),
      accidentTable.select("__id", fields:_*))

    accident.table.cache()

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

    roads.persist(StorageLevel.MEMORY_AND_DISK)

    val extraRoadNodeProps = Seq("Road_Type", "Road_Class")

    case class RefactorPair(df: DataFrame, nn: NestedNode)

    val thingsToRefactor = Seq(
      // Extract the "Accident_Severity" column as a property of a new node labeled
      // "Severity", so we end up with (:Accident)-[:SEVERITY]->(:Severity)
      RefactorPair(accidentTable, NestedNode(pk, "Severity", "Accident_Severity", "SEVERITY")),
      RefactorPair(accidentTable, NestedNode(pk, "LightCondition", "Light_Conditions", "CONDITION")),
      RefactorPair(accidentTable, NestedNode(pk, "WeatherCondition", "Weather_Conditions", "CONDITION")),
      RefactorPair(accidentTable, NestedNode(pk, "Date", "Date", "OCCURRED")),
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

    val ns: Seq[CAPSNodeTable] = parts.map(p => {
      p.nodes.table.cache()
      p.nodes
    })
    val es: Seq[CAPSRelationshipTable] = parts.map(_.edges).filter(_.isDefined).map(someRels => {
      val relTbl = someRels.get
      relTbl.table.cache()
      relTbl
    })

    val entityTables: Seq[CAPSEntityTable] = ns ++ es

    println("Assembling graph")
    val g = session.readFrom(accident, entityTables:_*).asCaps
    g.cache()
    g
  }
}
