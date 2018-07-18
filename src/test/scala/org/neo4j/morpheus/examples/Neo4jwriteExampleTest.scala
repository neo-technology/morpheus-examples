package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class Neo4jwriteExampleTest extends ExampleTest {
  it("runs Neo4jWriteExample") {
    validate(
      Neo4jWriteExample.main(Array.empty),
      getClass.getResource("/example_outputs/Neo4jWriteExample.out").toURI
    )
  }

}
