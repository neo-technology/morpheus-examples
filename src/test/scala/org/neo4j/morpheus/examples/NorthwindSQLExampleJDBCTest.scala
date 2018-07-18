package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class NorthwindSQLExampleJDBCTest extends ExampleTest {
  it("runs NorthwindSQLExampleJDBC") {
    validate(
      NorthwindSQLExampleJDBC.main(Array.empty),
      getClass.getResource("/example_outputs/NorthwindSQLExampleJDBC.out").toURI
    )
  }

}
