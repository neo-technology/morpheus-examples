package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class Neo4jAndParquetExampleTest extends ExampleTest {
  it("runs Neo4jAndParquetExample") {
    validate(Neo4jAndParquetExample.main(Array.empty), expectedOut =
      """|╔════════╤═════════════════╗
         |║ person │ product         ║
         |╠════════╪═════════════════╣
         |║ 'Bob'  │ '1984'          ║
         |║ 'Bob'  │ 'Jurassic Park' ║
         |║ 'Bob'  │ 'Shakira'       ║
         |║ 'Bob'  │ 'Terminator 2'  ║
         |╚════════╧═════════════════╝
         |(4 rows)
         |""".stripMargin)
  }

}
