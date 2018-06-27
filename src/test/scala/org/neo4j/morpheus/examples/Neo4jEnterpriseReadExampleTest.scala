package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class Neo4jEnterpriseReadExampleTest extends ExampleTest {
  it("runs Neo4jAndParquetExample") {
    validate(Neo4jEnterpriseReadExample.main(Array.empty), expectedOut =
      """|╔════════╤═════════════╤════════════╗
         |║ n.name │ type(r)     │ labels(m)  ║
         |╠════════╪═════════════╪════════════╣
         |║ 'Bob'  │ 'FRIEND_OF' │ ['Person'] ║
         |╚════════╧═════════════╧════════════╝
         |(1 row)
         |""".stripMargin)
  }

}
