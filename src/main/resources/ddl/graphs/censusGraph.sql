-- format for below is: <dataSourceName>.<schemaName>
SET SCHEMA CENSUS.CENSUS;

-- =================================================================

CREATE LABEL LicensedDog
  PROPERTIES (
    licence_number CHAR(10)
  )
  KEY
    LicensedDog_NK (LICENCE_NUMBER)

CREATE LABEL Person PROPERTIES LIKE view_person

CREATE LABEL Visitor
  PROPERTIES (
    date_of_entry DATE,
    sequence INTEGER,
    nationality CHAR(3),
    age INTEGER
  )
  KEY
    Visitor_NK (date_of_entry, sequence)

CREATE LABEL Resident
  PROPERTIES (
    person_number CHAR(10)
  )
  KEY
    Resident_NK (person_number)

CREATE LABEL Town
  PROPERTIES LIKE
    town -- a base table
  KEY
    Town_NK IS Town_PK

CREATE LABEL PRESENT_IN
	-- no properties

CREATE LABEL LICENSED_BY
  PROPERTIES (
    date_of_licence DATE
  )

-- =================================================================

CREATE GRAPH SCHEMA Census

  --NODES
  (Person, Visitor),  -- keyed by node key Visitor_NK
  (Person, Resident), -- keyed by node key Resident_NK
  (Town),
  (LicensedDog)

  --EDGES
  PRESENT_IN,
  [LICENSED_BY]

   --EDGE LABEL CONSTRAINTS
  (Person | LicensedDog) <0 .. *> - [PRESENT_IN] -> <1>(Town),
  (LicensedDog)- LICENSED_BY ->(Resident)

-- =================================================================

CREATE GRAPH Census_1901 WITH SCHEMA Census

---------
-- NODES
---------
	NODES LABELED
	  (Person, Visitor)
	FROM
	  view_visitor
  MAPPING TARGET
    visitor_MT (nationality, passport_number)


  NODES LABELLED
    (Person, Resident)
  FROM
    view_resident
  MAPPING TARGET
    enumerated_resident_MT (person_number)


  NODES LABELLED
    Town
  FROM
    town -- a base table
  MAPPING TARGET
    town_MT (region, city_name)


  NODES LABELLED
    LicensedDog
  FROM
    view_licensed_dog
  MAPPING TARGET
    LicensedDog_MT IS LicensedDog_NK


---------
-- EDGES
---------
  EDGES LABELLED PRESENT_IN
    FROM view_resident_enumerated_in_town
      MAPPING (person_number) ONTO
        enumerated_resident_MT
      FOR START NODE LABELLED
        (Person, Resident)
      MAPPING (region, city_name) ONTO
        town_MT
      FOR END NODE LABELLED
        (Town)

    FROM view_visitor_enumerated_in_town
      MAPPING (countryOfOrigin, passport_no) ONTO
        visitor_MT
      FOR START NODE
        (Person, Visitor)
      MAPPING (region, city_name) ONTO
        town  -- using input table name, since the column names match
      FOR END NODE LABELLED
        (Town)

    FROM view_licensed_dog
      MAPPING (licence_number)   -- node is edge, can omit ONTO phrase
      FOR START NODE
        (LicensedDog)
      MAPPING (region, city_name) ONTO
        town_MT
      FOR END NODE LABELLED
        (Town)


  EDGES LABELLED LICENSED_BY
    FROM view_licensed_dog
      MAPPING (licence_number) ONTO
        LicensedDog_MT
      FOR START NODE
        (LicensedDog)
      MAPPING (person_number) ONTO
        enumerated_resident_MT
      FOR END NODE
        (Person, Resident)