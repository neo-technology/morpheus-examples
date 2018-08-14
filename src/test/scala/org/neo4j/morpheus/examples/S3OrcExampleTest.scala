/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 */
package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class S3OrcExampleTest extends ExampleTest {

  it("Writes orc format successfully to S3 bucket") {
    validate(
      S3OrcWriteExample.main(Array.empty),
      getClass.getResource("/example_outputs/OrcWriteExample.out").toURI
    )
  }

  it("Read orc format successfully from S3 bucket") {
    validate(
      S3OrcReadExample.main(Array.empty),
      getClass.getResource("/example_outputs/S3ReadExample.out").toURI
    )
  }
}