/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class HiveSqlGraphSourceExampleTest extends ExampleTest {
  it("runs HiveSqlGraphSourceExample") {
    validate(
      HiveSqlGraphSourceExample.main(Array.empty),
      getClass.getResource("/example_outputs/HiveSqlGraphSourceExample.out").toURI
    )
  }
}
