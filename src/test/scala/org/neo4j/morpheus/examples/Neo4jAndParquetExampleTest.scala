/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class Neo4jAndParquetExampleTest extends ExampleTest {
  it("runs Neo4jAndParquetExample") {
    validate(
      Neo4jAndParquetExample.main(Array.empty),
      getClass.getResource("/example_outputs/Neo4jAndParquetExample.out").toURI
    )
  }

}
