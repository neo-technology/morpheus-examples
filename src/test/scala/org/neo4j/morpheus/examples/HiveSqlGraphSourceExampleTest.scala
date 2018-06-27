package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class HiveSqlGraphSourceExampleTest extends ExampleTest {
  it("runs HiveSqlGraphSourceExample") {
    validate(HiveSqlGraphSourceExample.main(Array.empty),
      """|CENSUS.TOWN table for HIVE successfully
         |CENSUS.RESIDENTS table for HIVE successfully
         |CENSUS.VISITORS table for HIVE successfully
         |CENSUS.LICENSED_DOGS table for HIVE successfully
         |╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╤═════════════════╤══════════════════════════════════════════════════════════╗
         |║ n                                                                                                                                                                │ r               │ m                                                        ║
         |╠══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╪═════════════════╪══════════════════════════════════════════════════════════╣
         |║ (:`Person`:`Visitor` {`age`: '35', `date_of_entry`: '1999-6-27', `first_name`: 'Simone', `last_name`: 'Brillot', `nationality`: 'FRA', `sequence`: '173'})       │ [:`PRESENT_IN`] │ (:`Town` {`CITY_NAME`: 'Beruna', `REGION`: 'Central'})   ║
         |║ (:`Person`:`Visitor` {`age`: '39', `date_of_entry`: '2000-7-9', `first_name`: 'James', `last_name`: 'Saravanamuthu', `nationality`: 'MYS', `sequence`: '844'})   │ [:`PRESENT_IN`] │ (:`Town` {`CITY_NAME`: 'Newtown', `REGION`: 'North'})    ║
         |║ (:`Person`:`Visitor` {`age`: '66', `date_of_entry`: '1942-2-3', `first_name`: 'Mary', `last_name`: 'Roe', `nationality`: 'CAN', `sequence`: '180'})              │ [:`PRESENT_IN`] │ (:`Town` {`CITY_NAME`: 'Newtown', `REGION`: 'North'})    ║
         |║ (:`Person`:`Visitor` {`age`: '63', `date_of_entry`: '1956-7-28', `first_name`: 'Kevin', `last_name`: 'Cooper', `nationality`: 'AUS', `sequence`: '942'})         │ [:`PRESENT_IN`] │ (:`Town` {`CITY_NAME`: 'Overdale', `REGION`: 'Central'}) ║
         |║ (:`Person`:`Visitor` {`age`: '67', `date_of_entry`: '1986-5-26', `first_name`: 'Angelo', `last_name`: 'Costas', `nationality`: 'ESP', `sequence`: '402'})        │ [:`PRESENT_IN`] │ (:`Town` {`CITY_NAME`: 'Treeside', `REGION`: 'West'})    ║
         |║ (:`Person`:`Visitor` {`age`: '62', `date_of_entry`: '1973-6-17', `first_name`: 'Barney', `last_name`: 'Odroglu', `nationality`: 'TUR', `sequence`: '471'})       │ [:`PRESENT_IN`] │ (:`Town` {`CITY_NAME`: 'Treeside', `REGION`: 'West'})    ║
         |║ (:`Person`:`Visitor` {`age`: '38', `date_of_entry`: '1966-3-25', `first_name`: 'Paolo', `last_name`: 'Terranova', `nationality`: 'BRA', `sequence`: '642'})      │ [:`PRESENT_IN`] │ (:`Town` {`CITY_NAME`: 'Treeside', `REGION`: 'West'})    ║
         |║ (:`Person`:`Visitor` {`age`: '36', `date_of_entry`: '1967-12-2', `first_name`: 'Hallgerd', `last_name`: 'Olafsdottir', `nationality`: 'ISL', `sequence`: '887'}) │ [:`PRESENT_IN`] │ (:`Town` {`CITY_NAME`: 'Dimrill', `REGION`: 'West'})     ║
         |╚══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╧═════════════════╧══════════════════════════════════════════════════════════╝
         |(8 rows)
         |""".stripMargin)
  }
}
