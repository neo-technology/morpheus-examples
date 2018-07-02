-- format for below is: <dataSourceName>.<schemaName>
SET SCHEMA H2.NORTHWIND;

-- Node labels

CREATE LABEL Employee PROPERTIES LIKE view_Employees
CREATE LABEL Territory PROPERTIES LIKE view_Territories
CREATE LABEL Supplier PROPERTIES LIKE view_Suppliers
CREATE LABEL Customer PROPERTIES LIKE view_Customers
CREATE LABEL Product PROPERTIES LIKE view_Products
CREATE LABEL OrderDetails PROPERTIES LIKE view_Order_Details
CREATE LABEL Category PROPERTIES LIKE view_Categories
CREATE LABEL Region PROPERTIES LIKE view_Region
CREATE LABEL Order PROPERTIES LIKE view_Orders
CREATE LABEL Shipper PROPERTIES LIKE view_Shippers
CREATE LABEL CustomerDemographic PROPERTIES LIKE view_CustomerDemographics

-- Relationship types

CREATE LABEL HAS_SUPPLIER
CREATE LABEL HAS_PRODUCT
CREATE LABEL HAS_CATEGORY
CREATE LABEL HAS_TERRITORY
CREATE LABEL HAS_EMPLOYEE
CREATE LABEL REPORTS_TO
CREATE LABEL HAS_CUSTOMER
CREATE LABEL HAS_CUSTOMER_DEMOGRAPHIC
CREATE LABEL HAS_ORDER
CREATE LABEL HAS_SHIPPER
CREATE LABEL HAS_REGION

-- =================================================================

CREATE GRAPH SCHEMA NORTHWIND_NAIVE

    -- Nodes
    (Employee),
    (Territory),
    (Supplier),
    (Customer),
    (Product),
    (OrderDetails),
    (Category),
    (Region),
    (Employee),
    (Order),
    (Shipper),
    (CustomerDemographic)

    -- Relationships
    [HAS_SUPPLIER],
    [HAS_PRODUCT],
    [HAS_CATEGORY],
    [HAS_TERRITORY],
    [HAS_EMPLOYEE],
    [REPORTS_TO],
    [HAS_CUSTOMER],
    [HAS_CUSTOMER_DEMOGRAPHIC],
    [HAS_ORDER],
    [HAS_SHIPPER],
    [HAS_REGION]

    -- Relationship type constraints
    (Product)-[HAS_SUPPLIER]->(Supplier),
    (Product)-[HAS_CATEGORY]->(Category),
    (OrderDetails)-[HAS_PRODUCT]->(Product),
    (OrderDetails)-[HAS_ORDER]->(Order),
    (Order)-[HAS_CUSTOMER]->(Customer),
    (Order)-[HAS_EMPLOYEE]->(Employee),
    (Order)-[HAS_SHIPPER]->(Shipper),
    (Employee)-[REPORTS_TO]->(Employee),
    (Territory)-[HAS_REGION]->(Region),
    -- Link tables become two relationships in either direction
    (Employee)-[HAS_TERRITORY]->(Territory),
    (Territory)-[HAS_EMPLOYEE]->(Employee),
    (Customer)-[HAS_CUSTOMER_DEMOGRAPHIC]->(CustomerDemographic),
    (CustomerDemographic)-[HAS_CUSTOMER]->(Customer)

-- =================================================================

CREATE GRAPH Northwind WITH SCHEMA NORTHWIND_NAIVE

  -- NODES

    -- (:Employee)
    NODES LABELLED (Employee)
    FROM view_Employees
      MAPPING TARGET employee_MT (EmployeeID)

    -- (:Territory)
    NODES LABELLED (Territory)
    FROM view_Territories
      MAPPING TARGET territory_MT (TerritoryID)

    -- (:Supplier)
    NODES LABELLED (Supplier)
    FROM view_Suppliers
      MAPPING TARGET supplier_MT (SupplierID)

    -- (:Customer)
    NODES LABELLED (Customer)
    FROM view_Customers
      MAPPING TARGET customer_MT (CustomerID)

    -- (:Product)
    NODES LABELLED (Product)
    FROM view_Products
      MAPPING TARGET product_MT (ProductID)

    -- (:Category)
    NODES LABELLED (Category)
    FROM view_Categories
      MAPPING TARGET category_MT (CategoryID)

    -- (:Region)
    NODES LABELLED (Region)
    FROM view_Region
      MAPPING TARGET region_MT (RegionID)

    -- (:Order)
    NODES LABELLED (Order)
    FROM view_Orders
      MAPPING TARGET order_MT (OrderID)

    -- (:Shipper)
    NODES LABELLED (Shipper)
    FROM view_Shippers
      MAPPING TARGET shipper_MT (ShipperID)

    -- (:CustomerDemographic)
    NODES LABELLED (CustomerDemographic)
    FROM view_CustomerDemographics
      MAPPING TARGET customer_demographics_MT (CustomerTypeID)

    -- (:OrderDetails)
    NODES LABELLED (OrderDetails)
    FROM view_Order_Details
      MAPPING TARGET order_details_MT (OrderID, ProductID)

  -- RELATIONSHIPS

    -- (:Employee)-[:HAS_TERRITORY]->(:Territory)
    EDGES LABELLED HAS_TERRITORY
    FROM view_EmployeeTerritories
      MAPPING (EmployeeID) ONTO employee_MT
        FOR START NODE LABELLED (Employee)
      MAPPING (TerritoryID) ONTO territory_MT
        FOR END NODE LABELLED (Territory)

    -- (:Territory)-[:HAS_EMPLOYEE]->(:Employee)
    EDGES LABELLED HAS_EMPLOYEE
    FROM view_EmployeeTerritories
      MAPPING (TerritoryID) ONTO territory_MT
        FOR START NODE LABELLED (Territory)
      MAPPING (EmployeeID) ONTO employee_MT
        FOR END NODE LABELLED (Employee)

    -- (:Order)-[:HAS_EMPLOYEE]->(:Employee)
    FROM view_Orders
      MAPPING (OrderID) ONTO order_MT
        FOR START NODE LABELLED (Order)
      MAPPING (EmployeeID) ONTO employee_MT
        FOR END NODE LABELLED (Employee)

    -- (:Employee)-[:REPORTS_TO]->(:Employee)
    EDGES LABELLED REPORTS_TO
    FROM view_Employees
      MAPPING (EmployeeID) ONTO employee_MT
        FOR START NODE LABELLED (Employee)
      MAPPING (ReportsTo) ONTO employee_MT
        FOR END NODE LABELLED (Employee)

    -- (:Product)-[:HAS_SUPPLIER]->(:Supplier)
    EDGES LABELLED HAS_SUPPLIER
    FROM view_Products
      MAPPING (ProductID) ONTO product_MT
        FOR START NODE LABELLED (Product)
      MAPPING (SupplierID) ONTO supplier_MT
        FOR END NODE LABELLED (Supplier)

    -- (Product)-[HAS_CATEGORY]->(Category)
    EDGES LABELLED HAS_CATEGORY
    FROM view_Products
      MAPPING (ProductID) ONTO product_MT
        FOR START NODE LABELLED (Product)
      MAPPING (CategoryID) ONTO category_MT
        FOR END NODE LABELLED (Category)

    -- (OrderDetails)-[HAS_PRODUCT]->(Product)
    EDGES LABELLED HAS_PRODUCT
    FROM view_Order_Details
      MAPPING (OrderID, ProductID) ONTO order_details_MT
        FOR START NODE LABELLED (OrderDetails)
      MAPPING (ProductID) ONTO product_MT
        FOR END NODE LABELLED (Product)

    -- (OrderDetails)-[HAS_ORDER]->(Order)
    EDGES LABELLED HAS_ORDER
    FROM view_Order_Details
      MAPPING (OrderID, ProductID) ONTO order_details_MT
        FOR START NODE LABELLED (OrderDetails)
      MAPPING (OrderID) ONTO order_MT
        FOR END NODE LABELLED (Order)

    -- (Order)-[HAS_CUSTOMER]->(Customer)
    EDGES LABELLED HAS_CUSTOMER
    FROM view_Orders
      MAPPING (OrderID) ONTO order_MT
        FOR START NODE LABELLED (Order)
      MAPPING (CustomerID) ONTO customer_MT
        FOR END NODE LABELLED (Customer)

    -- (CustomerDemographic)-[HAS_CUSTOMER]->(Customer)
    FROM CustomerCustomerDemo
      MAPPING (CustomerTypeID) ONTO customer_demographics_MT
        FOR START NODE LABELLED (CustomerDemographic)
      MAPPING (CustomerID) ONTO customer_MT
        FOR END NODE LABELLED (Customer)

    -- (Order)-[HAS_SHIPPER]->(Shipper)
    EDGES LABELLED HAS_SHIPPER
    FROM view_Orders
      MAPPING (OrderID) ONTO order_MT
        FOR START NODE LABELLED (Order)
      MAPPING (ShipVia) ONTO shipper_MT
        FOR END NODE LABELLED (Shipper)

    -- (Territory)-[HAS_REGION]->(Region)
    EDGES LABELLED HAS_REGION
    FROM view_Territories
      MAPPING (TerritoryID) ONTO territory_MT
        FOR START NODE LABELLED (Territory)
      MAPPING (RegionID) ONTO region_MT
        FOR END NODE LABELLED (Region)

    -- (Customer)-[HAS_CUSTOMER_DEMOGRAPHIC]->(CustomerDemographic)
    EDGES LABELLED HAS_CUSTOMER_DEMOGRAPHIC
    FROM CustomerCustomerDemo
      MAPPING (CustomerID) ONTO customer_MT
        FOR START NODE LABELLED (Customer)
      MAPPING (CustomerTypeID) ONTO customer_demographics_MT
        FOR END NODE LABELLED (CustomerDemographic)

