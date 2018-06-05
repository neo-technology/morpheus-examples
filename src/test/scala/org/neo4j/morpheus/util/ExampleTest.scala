package org.neo4j.morpheus.util

import java.io.ByteArrayOutputStream

import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

abstract class ExampleTest extends FunSpec with Matchers with BeforeAndAfterAll {

  private val oldStdOut = System.out

  protected def validate(app: => Unit, expectedOut: String): Unit = {
    val outCapture = new ByteArrayOutputStream()
    Console.withOut(outCapture)(app)
    outCapture.toString("UTF-8") shouldEqual expectedOut
  }

  override protected def afterAll(): Unit = {
    System.setOut(oldStdOut)
    super.afterAll()
  }
}