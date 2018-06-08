package org.neo4j.morpheus.examples.accidents

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.spark.api.io.CAPSNodeTable
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.spark.api.CAPSSession

import org.opencypher.spark.impl.CAPSConverters._

import scala.reflect.io.File

class Vehicles(csvDir:String)(implicit spark:SparkSession, session: CAPSSession) extends Graphable {
  val fields = Seq(
    "Accident_Index", "Vehicle_Reference", "Vehicle_Type", "Towing_and_Articulation",
    "Vehicle_Manoeuvre","Vehicle_Location-Restricted_Lane","Junction_Location",
    "Skidding_and_Overturning","Hit_Object_in_Carriageway","Vehicle_Leaving_Carriageway",
    "Hit_Object_off_Carriageway","1st_Point_of_Impact","Was_Vehicle_Left_Hand_Drive?",
    "Journey_Purpose_of_Driver",
    "Sex_of_Driver","Age_of_Driver","Age_Band_of_Driver","Engine_Capacity_(CC)",
    "Propulsion_Code","Age_of_Vehicle","Driver_IMD_Decile","Driver_Home_Area_Type",
    "Vehicle_IMD_Decile"
  )

  val df = spark.read
    .format("csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .load(csvDir + File.separator + "Veh.csv")
    .select(fields.head, fields.tail:_*)
    .withColumn(reservedId, curId)

  df.persist(StorageLevel.MEMORY_AND_DISK)

  def asGraph(label:String = "Vehicle") : PropertyGraph = {
    val mapping = NodeMapping.withSourceIdKey(reservedId)
    .withImpliedLabel("Vehicle")
    .withPropertyKeys(fields:_*)

    val nodeTable = CAPSNodeTable(mapping, df)
    nodeTable.table.cache()

    val g = session.readFrom(nodeTable).asCaps
    g.cache()
    g
  }
}
