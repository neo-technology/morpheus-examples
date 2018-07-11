package org.neo4j.morpheus.utils

import org.neo4j.graphdb.Result
import org.neo4j.harness.{ServerControls, TestServerBuilders}
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.opencypher.okapi.procedures.OkapiProcedures
import org.opencypher.spark.api.io.neo4j.Neo4jConfig

// TODO: this is copied from spark-cypher-testing, which we do not bundle with morpheus.
object Neo4jHarnessUtils {

  implicit class RichServerControls(val neo4j: ServerControls) extends AnyVal {

    def dataSourceConfig =
      Neo4jConfig(neo4j.boltURI(), user = "anonymous", password = Some("password"), encrypted = false)

    def uri: String = {
      val scheme = neo4j.boltURI().getScheme
      val userInfo = s"anonymous:password@"
      val host = neo4j.boltURI().getAuthority
      s"$scheme://$userInfo$host"
    }

    def stop(): Unit = {
      neo4j.close()
    }

    def execute(cypher: String): Result =
      neo4j.graph().execute(cypher)

    def withSchemaProcedure: ServerControls =
      withProcedure(classOf[OkapiProcedures])

    def withProcedure(procedures: Class[_]*): ServerControls = {
      val proceduresService = neo4j.graph()
        .asInstanceOf[GraphDatabaseAPI]
        .getDependencyResolver.
        resolveDependency(classOf[Procedures])

      for (procedure <- procedures) {
        proceduresService.registerProcedure(procedure, true)
        proceduresService.registerFunction(procedure, true)
        proceduresService.registerAggregationFunction(procedure, true)
      }
      neo4j
    }
  }

  def startNeo4j(dataFixture: String): ServerControls = {
    TestServerBuilders
      .newInProcessBuilder()
      .withConfig("dbms.security.auth_enabled", "true")
      .withFixture("CALL dbms.security.createUser('anonymous', 'password', false)")
      .withFixture(dataFixture)
      .newServer()
      .withSchemaProcedure
  }

}
