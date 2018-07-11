package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class Neo4jReadExampleTest extends ExampleTest {
  it("runs Neo4jReadExample") {
    validate(Neo4jReadExample.main(Array.empty), expectedOut =
      """|╔════════╤═════════════╤════════════╗
         |║ n.name │ type(r)     │ labels(m)  ║
         |╠════════╪═════════════╪════════════╣
         |║ 'Bob'  │ 'FRIEND_OF' │ ['Person'] ║
         |╚════════╧═════════════╧════════════╝
         |(1 row)
         |""".stripMargin)
  }

}
