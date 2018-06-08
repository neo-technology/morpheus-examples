package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class Neo4jEnterpriseWriteExampleTest extends ExampleTest {
  it("runs Neo4jAndParquetExample") {
    validate(Neo4jEnterpriseWriteExample.main(Array.empty), expectedOut =
      """|+--------------------------------+
         || n.name  | m.name  | confidence |
         |+--------------------------------+
         || 'Alice' | 'Carol' | 1          |
         |+--------------------------------+
         |(1 rows)
         |""".stripMargin)
  }

}
