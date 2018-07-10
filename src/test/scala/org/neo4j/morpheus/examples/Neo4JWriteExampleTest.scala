package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class Neo4JWriteExampleTest extends ExampleTest {
  it("runs Neo4jAndParquetExample") {
    validate(Neo4jWriteExample.main(Array.empty), expectedOut =
      """|╔═════════╤═════════╤════════════╗
         |║ n.name  │ m.name  │ confidence ║
         |╠═════════╪═════════╪════════════╣
         |║ 'Alice' │ 'Carol' │ 1          ║
         |╚═════════╧═════════╧════════════╝
         |(1 row)
         |""".stripMargin)
  }

}
