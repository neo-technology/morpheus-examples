package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class JdbcSqlGraphSourceExampleTest extends ExampleTest {
  it("runs JdbcSqlGraphSourceExample") {
    validate(JdbcSqlGraphSourceExample.main(Array.empty),
      """|CENSUS.TOWN table for jdbc successfully
         |CENSUS.RESIDENTS table for jdbc successfully
         |CENSUS.FOREIGNERS table for jdbc successfully
         |CENSUS.DOGS table for jdbc successfully
         |+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         || n                                                                                                                                                | r             | m                                                  |
         |+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         || (:Person:Visitor {FIRST_NAME: 'Simone', LAST_NAME: 'Brillot', age: '35', date_of_entry: '1999-6-27', nationality: 'FRA', sequence: '173'})       | [:PRESENT_IN] | (:Town {CITY_NAME: 'Beruna', REGION: 'Central'})   |
         || (:Person:Visitor {FIRST_NAME: 'James', LAST_NAME: 'Saravanamuthu', age: '39', date_of_entry: '2000-7-9', nationality: 'MYS', sequence: '844'})   | [:PRESENT_IN] | (:Town {CITY_NAME: 'Newtown', REGION: 'North'})    |
         || (:Person:Visitor {FIRST_NAME: 'Mary', LAST_NAME: 'Roe', age: '66', date_of_entry: '1942-2-3', nationality: 'CAN', sequence: '180'})              | [:PRESENT_IN] | (:Town {CITY_NAME: 'Newtown', REGION: 'North'})    |
         || (:Person:Visitor {FIRST_NAME: 'Kevin', LAST_NAME: 'Cooper', age: '63', date_of_entry: '1956-7-28', nationality: 'AUS', sequence: '942'})         | [:PRESENT_IN] | (:Town {CITY_NAME: 'Overdale', REGION: 'Central'}) |
         || (:Person:Visitor {FIRST_NAME: 'Angelo', LAST_NAME: 'Costas', age: '67', date_of_entry: '1986-5-26', nationality: 'ESP', sequence: '402'})        | [:PRESENT_IN] | (:Town {CITY_NAME: 'Treeside', REGION: 'West'})    |
         || (:Person:Visitor {FIRST_NAME: 'Barney', LAST_NAME: 'Odroglu', age: '62', date_of_entry: '1973-6-17', nationality: 'TUR', sequence: '471'})       | [:PRESENT_IN] | (:Town {CITY_NAME: 'Treeside', REGION: 'West'})    |
         || (:Person:Visitor {FIRST_NAME: 'Paolo', LAST_NAME: 'Terranova', age: '38', date_of_entry: '1966-3-25', nationality: 'BRA', sequence: '642'})      | [:PRESENT_IN] | (:Town {CITY_NAME: 'Treeside', REGION: 'West'})    |
         || (:Person:Visitor {FIRST_NAME: 'Hallgerd', LAST_NAME: 'Olafsdottir', age: '36', date_of_entry: '1967-12-2', nationality: 'ISL', sequence: '887'}) | [:PRESENT_IN] | (:Town {CITY_NAME: 'Dimrill', REGION: 'West'})     |
         |+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |(8 rows)
         |""".stripMargin)
  }
}
