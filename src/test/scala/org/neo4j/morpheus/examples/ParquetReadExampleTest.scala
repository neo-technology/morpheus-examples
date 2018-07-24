/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class ParquetReadExampleTest extends ExampleTest {
  it("runs ParquetReadExample") {
    validate(
      ParquetReadExample.main(Array.empty),
      getClass.getResource("/example_outputs/ParquetReadExample.out").toURI
    )
  }

}
