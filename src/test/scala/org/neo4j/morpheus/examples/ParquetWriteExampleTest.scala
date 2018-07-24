/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class ParquetWriteExampleTest extends ExampleTest {
  it("runs ParquetWriteExample") {
    validate(
      ParquetWriteExample.main(Array.empty),
      getClass.getResource("/example_outputs/ParquetWriteExample.out").toURI
    )
  }

}
