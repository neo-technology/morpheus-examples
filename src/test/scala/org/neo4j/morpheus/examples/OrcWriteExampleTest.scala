package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class OrcWriteExampleTest extends ExampleTest {
  it("runs Neo4jAndParquetExample") {
    validate(OrcWriteExample.main(Array.empty), "")
  }

}
