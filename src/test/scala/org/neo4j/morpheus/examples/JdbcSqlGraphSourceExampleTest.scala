package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class JdbcSqlGraphSourceExampleTest extends ExampleTest {
  it("runs JdbcSqlGraphSourceExample") {
    validate(
      JdbcSqlGraphSourceExample.main(Array.empty),
      getClass.getResource("/example_outputs/JdbcSqlGraphSourceExample.out").toURI
    )
  }
}
