package org.neo4j.morpheus.examples.accidents

import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.spark.api.io.CAPSNodeTable
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.spark.api.CAPSSession

import org.apache.spark.sql.functions.{concat, lit}

import scala.reflect.io.File

class CarMakeModel(csvDir:String)(implicit spark:SparkSession, session: CAPSSession) extends Graphable {
  val fields = Seq(
    "Accident_Index","accyr","Vehicle_Reference",
    "Vehicle_Type","Towing_and_Articulation",
    "Vehicle_Manoeuvre","Vehicle_Location-Restricted_Lane",
    "Junction_Location","Skidding_and_Overturning",
    "Hit_Object_in_Carriageway","Vehicle_Leaving_Carriageway",
    "Hit_Object_off_Carriageway","1st_Point_of_Impact",
    "Was_Vehicle_Left_Hand_Drive","Journey_Purpose_of_Driver",
    "Sex_of_Driver","Age_Band_of_Driver","Engine_Capacity_(CC)",
    "Propulsion_Code","Age_of_Vehicle","Driver_IMD_Decile",
    "Driver_Home_Area_Type","make","model"
  )

  val dfTmp = spark.read
    .format("csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .load(csvDir + File.separator + "MakeModel2016.csv")
    .select(fields.head, fields.tail:_*)

  val df = dfTmp
    .withColumn("Trade_Name", concat(dfTmp.col("make"), lit(" "), dfTmp.col("model")))
    .withColumn(reservedId, curId)

  def asGraph(label:String = "MakeModel") : PropertyGraph = {
    val mapping = NodeMapping.withSourceIdKey(reservedId)
      .withImpliedLabel(label)
      .withPropertyKeys((Seq("Trade_Name") ++ fields):_*)

    session.readFrom(CAPSNodeTable(mapping, df))
  }
}
