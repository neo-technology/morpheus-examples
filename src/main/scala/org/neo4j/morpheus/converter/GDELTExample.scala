/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
package org.neo4j.morpheus.converter

import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}

/**
  * Imports CSV data from https://www.gdeltproject.org/ into DataFrames and constructs a graph.
  * Actors are represented as nodes, Events are represented as relationships.
  *
  * Run the program using the path to the csv as argument.
  */
object GDELTExample extends App {

  implicit val caps: CAPSSession = CAPSSession.local()

  val spark = caps.sparkSession

  // used for easy column lookups: $"column_name"
  import spark.implicits._

  val raw = spark.read
    .option("header", value = true)
    // based on sampling, could be made explicit
    .option("inferSchema", value = true)
    // path to GDELT csv file
    .csv(args(0))

  // read node data
  val inputNodes = raw
    .select(raw.aliasedActorCols("Actor1"): _*)
    .union(raw.select(raw.aliasedActorCols("Actor2"): _*))
    .distinct()
    .withColumn("id", functions.monotonically_increasing_id())

  // read rel data
  val relCols = raw.columns.map(raw.col).toSet --
    raw.actorCols("Actor1") --
    raw.actorCols("Actor2") +
    raw.col("Actor1Code") +
    raw.col("Actor2Code")

  val inputRels = inputNodes
    .select("ActorCode", "id")
    .join(raw.select(relCols.toSeq: _*), $"ActorCode" === $"Actor1Code")
    .withColumnRenamed("id", "source")
    .drop("ActorCode", "Actor1Code")
    .join(inputNodes.select("ActorCode", "id"), $"Actor2Code" === $"ActorCode")
    .withColumnRenamed("id", "target")
    .drop("ActorCode", "Actor2Code")
    .withColumn("id", functions.monotonically_increasing_id())

  // init CAPS graph from input data
  val g = caps.readFrom(CAPSNodeTable(Set("Actor"), inputNodes), CAPSRelationshipTable("EVENT", inputRels))

  // run Cypher
  val res = g.cypher(
    """
      |MATCH (n:Actor)-[r]->(m:Actor)
      |RETURN
      |n.ActorName AS actor1,
      |m.ActorName AS met,
      |r.ActionGeo_FullName AS in""".stripMargin)

  res.show

  //  +---------------------------------------------------------+
  //  | actor1     | met               | in                     |
  //  +---------------------------------------------------------+
  //  | 'EUROPEAN' | 'THE WHITE HOUSE' | 'Tehran, Tehran, Iran' |
  //  +---------------------------------------------------------+
  //  (1 rows)


  implicit class GDELTHelpers(df: DataFrame) {

    // actors are defined by columns starting with "ActorX"
    // "ActorXGeo" columns are part of the event
    def actorCols(s: String): Array[Column] = df.columns
      .filter(_.startsWith(s))
      .filterNot(_.startsWith(s"${s}Geo"))
      .sorted
      .map(df.col)

    def aliasedActorCols(s: String): Array[Column] = df.actorCols(s)
      .map(c => c.toString() -> c)
      .map { case (name, col) => col.as(name.replace(s, "Actor")) }
  }
}
