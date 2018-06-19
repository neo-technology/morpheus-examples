package org.neo4j.morpheus.examples

import java.nio.file.{Path, Paths}

import org.neo4j.morpheus.utils.{ConsoleApp, NorthwindDB}
import com.neo4j.sql.SqlGraphSource
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

object NorthwindSQLExampleJDBC extends ConsoleApp {

  // start up the SQL database
  NorthwindDB.init()

  // Initialise local CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // define the root configuration directory for the SQL graph source
  // this holds the data source mappings files and the SQL DDL file
  // the latter contains the graph definitions and mappings from SQL tables that fill the graph with data
  private val rootDir: Path = Paths.get(getClass.getResource("/sql/pgds").toURI)

  // register the SQL graph source with the session
  session.registerSource(Namespace("sql"), SqlGraphSource(rootDir, "northwind.ddl.sql", "h2-source.json"))

  // print the number of nodes in the graph
  session.cypher(
    """
      |FROM GRAPH sql.Northwind
      |MATCH (n)
      |RETURN count(n)
    """.stripMargin).show

  // print the schema of the graph
  println(session.catalog.graph("sql.Northwind").schema.pretty)

  // run a simple query
  session.cypher(
    """
      |FROM GRAPH sql.Northwind
      |MATCH (e:Employee)-[:REPORTS_TO]->(:Employee)<-[:HAS_EMPLOYEE]-(o:Order)
      |RETURN o.CUSTOMERID, o.ORDERDATE, e.TITLE
      |  ORDER BY o.ORDERDATE
      |  LIMIT 50
    """.stripMargin).show

}
