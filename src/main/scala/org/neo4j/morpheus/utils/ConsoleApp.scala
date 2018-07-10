package org.neo4j.morpheus.utils

import org.opencypher.okapi.impl.util.PrintOptions

// TODO: this exists in spark-cypher-examples. Maybe move to spark-cypher-testing instead?
abstract class ConsoleApp extends App {

  // Redirects output to the current Console. This is needed for testing.
  implicit val printOptions: PrintOptions = PrintOptions(Console.out)

}
