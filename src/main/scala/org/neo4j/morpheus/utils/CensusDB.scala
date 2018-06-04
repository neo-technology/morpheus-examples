package org.neo4j.morpheus.utils

import java.sql.DriverManager

import org.apache.spark.sql.SparkSession
import org.neo4j.sql.SparkOptionKeys.{DB_TABLE, DRIVER, URL}
import org.neo4j.hadoop.{JdbcFormat, ParquetFormat, StorageFormat}

object CensusDB {

  def createJdbcData(driver: String, jdbcUrl: String, schema: String)(implicit sparkSession: SparkSession): Unit = {

    def executeDDL(ddlSeq: Seq[String]): Unit = {
      val conn = DriverManager.getConnection(jdbcUrl)
      conn.setSchema(schema)
      val stmt = conn.createStatement()
      ddlSeq.foreach(stmt.executeUpdate)
    }

    // Create schema/database
    Class.forName(driver)
    executeDDL(Seq(s"DROP SCHEMA IF EXISTS $schema", s"CREATE SCHEMA IF NOT EXISTS $schema", s"SET SCHEMA $schema"))

    // populate the data
    val baseOptions = Map(URL -> jdbcUrl, DRIVER -> driver)
    populateData("TOWN", "/csv/census/town.csv", JdbcFormat, schema, baseOptions ++ Map(DB_TABLE -> s"$schema.TOWN"))
    populateData("RESIDENTS", "/csv/census/residents.csv", JdbcFormat, schema, baseOptions ++ Map(DB_TABLE -> s"$schema.RESIDENTS"))
    populateData("FOREIGNERS", "/csv/census/foreigners.csv", JdbcFormat, schema, baseOptions ++ Map(DB_TABLE -> s"$schema.FOREIGNERS"))
    populateData("DOGS", "/csv/census/dogs.csv", JdbcFormat, schema, baseOptions ++ Map(DB_TABLE -> s"$schema.DOGS"))

    // create the views
    executeDDL(Seq(
      viewPerson(schema),
      viewVisitor(schema),
      viewResident(schema),
      viewLicensedDog(schema),
      viewResidentEnumInTown(schema),
      viewVisitorEnumInTown(schema)))
  }

  def createHiveData(schema: String)(implicit sparkSession: SparkSession): Unit = {

    // Create the database
    sparkSession.sql(s"create database $schema").count

    def executeDDL(ddlSeq: Seq[String]): Unit = {
      ddlSeq.foreach(sparkSession.sql(_).count)
    }

    populateData("TOWN", "/csv/census/town.csv", ParquetFormat, schema, Map.empty)
    populateData("RESIDENTS", "/csv/census/residents.csv", ParquetFormat, schema, Map.empty)
    populateData("FOREIGNERS", "/csv/census/foreigners.csv", ParquetFormat, schema, Map.empty)
    populateData("DOGS", "/csv/census/dogs.csv", ParquetFormat, schema, Map.empty)

    // create the views
    executeDDL(Seq(
      viewPerson(schema),
      viewVisitor(schema),
      viewResident(schema),
      viewLicensedDog(schema),
      viewResidentEnumInTown(schema),
      viewVisitorEnumInTown(schema)))
  }


  private def viewPerson(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_person AS
       |  SELECT
       |    first_name,
       |    last_name
       |  FROM
       |    $schema.residents
    """.stripMargin

  private def viewVisitor(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_visitor AS
       |  SELECT
       |    first_name,
       |    last_name,
       |    iso3166 as nationality,
       |    passport_number,
       |    date_of_entry,
       |    entry_sequence as sequence,
       |    age
       |  FROM
       |    $schema.foreigners
    """.stripMargin

  private def viewResident(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_resident AS
       |  SELECT
       |    first_name,
       |    last_name,
       |    person_number
       |  FROM
       |    $schema.residents
    """.stripMargin

  private def viewLicensedDog(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_licensed_dog AS
       |  SELECT
       |    person_number,
       |    licence_number,
       |    licence_date as date_of_licence,
       |    region,
       |    city_name
       |  FROM
       |    $schema.dogs
    """.stripMargin

  private def viewResidentEnumInTown(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_resident_enumerated_in_town AS
       |  SELECT
       |    PERSON_NUMBER,
       |    REGION,
       |    CITY_NAME
       |  FROM
       |    $schema.residents
    """.stripMargin

  private def viewVisitorEnumInTown(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_visitor_enumerated_in_town AS
       |  SELECT
       |    ISO3166 AS countryOfOrigin,
       |    PASSPORT_NUMBER AS PASSPORT_NO,
       |    REGION,
       |    CITY_NAME
       |  FROM
       |    $schema.foreigners
    """.stripMargin

  private def populateData(
    tableName: String,
    csvFilePath: String,
    storageFormat: StorageFormat,
    schema: String,
    options: Map[String, String]
  )(implicit sparkSession: SparkSession): Unit = {
    val writer = sparkSession
      .read
      .option("header", "true")
      .csv(getClass.getResource(s"$csvFilePath").getPath)
      .write
      .format(storageFormat.name)
      .options(options)
      .mode("ignore")
    if (storageFormat == JdbcFormat) writer.save
    else writer.saveAsTable(s"$schema.$tableName")
    println(s"$schema.$tableName table for ${if (storageFormat == ParquetFormat) "HIVE" else storageFormat.name} successfully")
  }

}
