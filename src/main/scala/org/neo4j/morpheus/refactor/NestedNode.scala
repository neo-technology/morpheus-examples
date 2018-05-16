package org.neo4j.morpheus.refactor

/**
  * Specification of a nested node in a home dataframe
  * @param pk primary key of the input DF
  * @param nestedLabel label for the nested node
  * @param nestedKey pk for the nested node
  * @param relType what kind of relation should link home -> nested
  * @param extraProps any extra properties of the DF that belong to the nested node
  */
case class NestedNode(pk:String, nestedLabel: String, nestedKey: String, relType: String, extraProps: String*)

