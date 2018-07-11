package org.neo4j.morpheus.examples

import org.neo4j.morpheus.util.ExampleTest

class NorthwindSQLExampleJDBCTest extends ExampleTest {
  it("runs NorthwindSQLExampleJDBC") {
    validate(NorthwindSQLExampleJDBC.main(Array.empty), expectedOut =
      """|╔══════════╗
         |║ count(n) ║
         |╠══════════╣
         |║ 3259     ║
         |╚══════════╝
         |(1 row)
         |Node labels {
         |	:Order
         |		ORDERID: INTEGER?
         |		SHIPCITY: STRING?
         |		SHIPPEDDATE: STRING?
         |		ORDERDATE: STRING?
         |		CUSTOMERID: STRING?
         |		REQUIREDDATE: STRING?
         |		SHIPNAME: STRING?
         |		SHIPCOUNTRY: STRING?
         |		FREIGHT: INTEGER?
         |		SHIPPOSTALCODE: STRING?
         |		SHIPVIA: INTEGER?
         |		SHIPREGION: STRING?
         |		SHIPADDRESS: STRING?
         |		EMPLOYEEID: INTEGER?
         |	:CustomerDemographic
         |		CUSTOMERTYPEID: STRING?
         |		CUSTOMERDESC: STRING?
         |	:OrderDetails
         |		PRODUCTID: INTEGER?
         |		ORDERID: INTEGER?
         |		UNITPRICE: INTEGER?
         |		QUANTITY: INTEGER?
         |		DISCOUNT: INTEGER?
         |	:Product
         |		PRODUCTID: INTEGER?
         |		REORDERLEVEL: INTEGER?
         |		UNITPRICE: INTEGER?
         |		CATEGORYID: INTEGER?
         |		PRODUCTNAME: STRING?
         |		QUANTITYPERUNIT: STRING?
         |		SUPPLIERID: INTEGER?
         |		DISCONTINUED: INTEGER?
         |		UNITSINSTOCK: INTEGER?
         |		UNITSONORDER: INTEGER?
         |	:Employee
         |		ADDRESS: STRING?
         |		REPORTSTO: INTEGER?
         |		EXTENSION: STRING?
         |		CITY: STRING?
         |		PHOTOPATH: STRING?
         |		HOMEPHONE: STRING?
         |		TITLE: STRING?
         |		REGION: STRING?
         |		FIRSTNAME: STRING?
         |		LASTNAME: STRING?
         |		TITLEOFCOURTESY: STRING?
         |		POSTALCODE: STRING?
         |		EMPLOYEEID: INTEGER?
         |		HIREDATE: STRING?
         |		COUNTRY: STRING?
         |		BIRTHDATE: STRING?
         |	:Category
         |		CATEGORYID: INTEGER?
         |		CATEGORYNAME: STRING?
         |		DESCRIPTION: STRING?
         |	:Supplier
         |		CONTACTNAME: STRING?
         |		ADDRESS: STRING?
         |		CITY: STRING?
         |		SUPPLIERID: INTEGER?
         |		CONTACTTITLE: STRING?
         |		REGION: STRING?
         |		PHONE: STRING?
         |		HOMEPAGE: STRING?
         |		FAX: STRING?
         |		POSTALCODE: STRING?
         |		COMPANYNAME: STRING?
         |		COUNTRY: STRING?
         |	:Territory
         |		TERRITORYID: STRING?
         |		TERRITORYDESCRIPTION: STRING?
         |		REGIONID: INTEGER?
         |	:Shipper
         |		SHIPPERID: INTEGER?
         |		COMPANYNAME: STRING?
         |		PHONE: STRING?
         |	:Customer
         |		CONTACTNAME: STRING?
         |		ADDRESS: STRING?
         |		CUSTOMERID: STRING?
         |		CITY: STRING?
         |		CONTACTTITLE: STRING?
         |		REGION: STRING?
         |		PHONE: STRING?
         |		FAX: STRING?
         |		POSTALCODE: STRING?
         |		COMPANYNAME: STRING?
         |		COUNTRY: STRING?
         |	:Region
         |		REGIONID: INTEGER?
         |}
         |no label implications
         |Rel types {
         |	:REPORTS_TO
         |	:HAS_CUSTOMER
         |	:HAS_ORDER
         |	:HAS_EMPLOYEE
         |	:HAS_SUPPLIER
         |	:HAS_SHIPPER
         |	:HAS_PRODUCT
         |	:HAS_REGION
         |	:HAS_CATEGORY
         |	:HAS_TERRITORY
         |	:HAS_CUSTOMER_DEMOGRAPHIC
         |}
         |
         |╔══════════════╤═════════════╤════════════════════════════╗
         |║ o.CUSTOMERID │ o.ORDERDATE │ e.TITLE                    ║
         |╠══════════════╪═════════════╪════════════════════════════╣
         |║ 'LAUGB'      │ '1/1/1998'  │ 'Sales Representative'     ║
         |║ 'LAUGB'      │ '1/1/1998'  │ 'Sales Representative'     ║
         |║ 'LAUGB'      │ '1/1/1998'  │ 'Sales Representative'     ║
         |║ 'LAUGB'      │ '1/1/1998'  │ 'Sales Manager'            ║
         |║ 'LAUGB'      │ '1/1/1998'  │ 'Inside Sales Coordinator' ║
         |║ 'OLDWO'      │ '1/1/1998'  │ 'Sales Representative'     ║
         |║ 'OLDWO'      │ '1/1/1998'  │ 'Sales Representative'     ║
         |║ 'OLDWO'      │ '1/1/1998'  │ 'Sales Representative'     ║
         |║ 'OLDWO'      │ '1/1/1998'  │ 'Sales Manager'            ║
         |║ 'OLDWO'      │ '1/1/1998'  │ 'Inside Sales Coordinator' ║
         |║ 'FAMIA'      │ '1/14/1997' │ 'Sales Representative'     ║
         |║ 'FAMIA'      │ '1/14/1997' │ 'Sales Representative'     ║
         |║ 'FAMIA'      │ '1/14/1997' │ 'Sales Representative'     ║
         |║ 'FAMIA'      │ '1/14/1997' │ 'Sales Manager'            ║
         |║ 'FAMIA'      │ '1/14/1997' │ 'Inside Sales Coordinator' ║
         |║ 'LAMAI'      │ '1/14/1998' │ 'Sales Representative'     ║
         |║ 'LAMAI'      │ '1/14/1998' │ 'Sales Representative'     ║
         |║ 'LAMAI'      │ '1/14/1998' │ 'Sales Representative'     ║
         |║ 'LAMAI'      │ '1/14/1998' │ 'Sales Manager'            ║
         |║ 'LAMAI'      │ '1/14/1998' │ 'Inside Sales Coordinator' ║
         |║ 'REGGC'      │ '1/2/1998'  │ 'Sales Representative'     ║
         |║ 'REGGC'      │ '1/2/1998'  │ 'Sales Representative'     ║
         |║ 'REGGC'      │ '1/2/1998'  │ 'Sales Representative'     ║
         |║ 'SUPRD'      │ '1/20/1998' │ 'Sales Representative'     ║
         |║ 'SUPRD'      │ '1/20/1998' │ 'Sales Representative'     ║
         |║ 'SUPRD'      │ '1/20/1998' │ 'Sales Representative'     ║
         |║ 'FRANS'      │ '1/22/1997' │ 'Sales Representative'     ║
         |║ 'FRANS'      │ '1/22/1997' │ 'Sales Representative'     ║
         |║ 'FRANS'      │ '1/22/1997' │ 'Sales Representative'     ║
         |║ 'FRANS'      │ '1/22/1997' │ 'Sales Manager'            ║
         |║ 'FRANS'      │ '1/22/1997' │ 'Inside Sales Coordinator' ║
         |║ 'SUPRD'      │ '1/22/1998' │ 'Sales Representative'     ║
         |║ 'SUPRD'      │ '1/22/1998' │ 'Sales Representative'     ║
         |║ 'SUPRD'      │ '1/22/1998' │ 'Sales Representative'     ║
         |║ 'SUPRD'      │ '1/22/1998' │ 'Sales Manager'            ║
         |║ 'SUPRD'      │ '1/22/1998' │ 'Inside Sales Coordinator' ║
         |║ 'RICAR'      │ '1/26/1998' │ 'Sales Representative'     ║
         |║ 'RICAR'      │ '1/26/1998' │ 'Sales Representative'     ║
         |║ 'RICAR'      │ '1/26/1998' │ 'Sales Representative'     ║
         |║ 'LACOR'      │ '1/29/1998' │ 'Sales Representative'     ║
         |║ 'LACOR'      │ '1/29/1998' │ 'Sales Representative'     ║
         |║ 'LACOR'      │ '1/29/1998' │ 'Sales Representative'     ║
         |║ 'LACOR'      │ '1/29/1998' │ 'Sales Manager'            ║
         |║ 'LACOR'      │ '1/29/1998' │ 'Inside Sales Coordinator' ║
         |║ 'MAGAA'      │ '1/3/1997'  │ 'Sales Representative'     ║
         |║ 'MAGAA'      │ '1/3/1997'  │ 'Sales Representative'     ║
         |║ 'MAGAA'      │ '1/3/1997'  │ 'Sales Representative'     ║
         |║ 'MAGAA'      │ '1/3/1997'  │ 'Sales Manager'            ║
         |║ 'MAGAA'      │ '1/3/1997'  │ 'Inside Sales Coordinator' ║
         |║ 'SAVEA'      │ '1/5/1998'  │ 'Sales Representative'     ║
         |╚══════════════╧═════════════╧════════════════════════════╝
         |(50 rows)
         |""".stripMargin)
  }

}
