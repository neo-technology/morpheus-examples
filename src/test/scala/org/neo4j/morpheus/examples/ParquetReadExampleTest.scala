package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class ParquetReadExampleTest extends ExampleTest {
  it("runs Neo4jAndParquetExample") {
    validate(ParquetReadExample.main(Array.empty),
      """|+----------------------------------------------------------------------------------+
         || n                                  | r        | m                                |
         |+----------------------------------------------------------------------------------+
         || (:Person {age: 23, name: 'Bob'})   | [:KNOWS] | (:Person {age: 84, name: 'Eve'}) |
         || (:Person {age: 42, name: 'Alice'}) | [:KNOWS] | (:Person {age: 23, name: 'Bob'}) |
         |+----------------------------------------------------------------------------------+
         |(2 rows)
         |""".stripMargin)
  }

}
