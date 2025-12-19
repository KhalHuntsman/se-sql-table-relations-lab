# STEP 0
# --------------------------------------------------
# Import required libraries:
# - sqlite3 for database connection
# - pandas for executing SQL queries and storing results as DataFrames
import sqlite3
import pandas as pd

# Establish a connection to the SQLite database file
conn = sqlite3.connect('data.sqlite')

# Optional: inspect database metadata (tables, indexes, etc.)
pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# Retrieve and print all table names in the database
# This helps confirm what tables are available before writing queries
df_tables = pd.read_sql(
    "SELECT name FROM sqlite_master WHERE type='table';",
    conn
)
print(df_tables)


# STEP 1
# --------------------------------------------------
# Find all employees who work in the Boston office.
# This uses an INNER JOIN because we only want employees
# that are associated with an office.
df_boston = pd.read_sql("""
    SELECT
        e.firstName,
        e.lastName
    FROM employees e
    JOIN offices o
        ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston';
""", conn)


# STEP 2
# --------------------------------------------------
# Identify offices that have zero employees.
# LEFT JOIN ensures all offices are included.
# Offices with no employees will have NULL employee fields.
df_zero_emp = pd.read_sql("""
    SELECT
        o.officeCode,
        o.city,
        o.state,
        o.country
    FROM offices o
    LEFT JOIN employees e
        ON o.officeCode = e.officeCode
    WHERE e.employeeNumber IS NULL
""", conn)


# STEP 3
# --------------------------------------------------
# Return all employees along with the city and state of their office.
# LEFT JOIN ensures employees without an office are still included.
# Results are ordered alphabetically by employee name.
df_employee = pd.read_sql("""
    SELECT
        e.firstName,
        e.lastName,
        o.city,
        o.state
    FROM employees e
    LEFT JOIN offices o
        ON e.officeCode = o.officeCode
    ORDER BY 
        e.firstName, 
        e.lastName
""", conn)


# STEP 4
# --------------------------------------------------
# Identify customers who have not placed any orders.
# LEFT JOIN keeps all customers.
# Customers without orders will have NULL order numbers.
df_contacts = pd.read_sql("""
    SELECT
        c.contactFirstName,
        c.contactLastName,
        c.phone,
        c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o
        ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName
""", conn)


# STEP 5
# --------------------------------------------------
# Produce a report of customer contacts and their payment details.
# INNER JOIN ensures only customers with payments are included.
# CAST ensures numeric sorting even if amount is stored as text.
df_payment = pd.read_sql("""
    SELECT
        c.contactFirstName,
        c.contactLastName,
        p.amount,
        p.paymentDate
    FROM customers c
    INNER JOIN payments p
        ON c.customerNumber = p.customerNumber
    ORDER BY CAST(p.amount AS REAL) DESC
""", conn)


# STEP 6
# --------------------------------------------------
# Identify the top 4 employees whose customers have an
# average credit limit greater than 90,000.
# Aggregation requires GROUP BY and HAVING.
df_credit = pd.read_sql("""
    SELECT
        e.employeeNumber,
        e.firstName,
        e.lastName,
        COUNT(c.customerNumber) AS num_customers
    FROM employees e
    JOIN customers c
        ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY
        e.employeeNumber,
        e.firstName,
        e.lastName
    HAVING AVG(c.creditLimit) > 90000
    ORDER BY num_customers DESC
    LIMIT 4
""", conn)


# STEP 7
# --------------------------------------------------
# Determine which products sell the most.
# - numorders: how many order records reference the product
# - totalunits: total quantity sold across all orders
df_product_sold = pd.read_sql("""
    SELECT
        p.productName,
        COUNT(o.productCode) AS numorders,
        SUM(o.quantityOrdered) AS totalunits
    FROM products p
    JOIN orderdetails o
        ON p.productCode = o.productCode
    GROUP BY p.productName
    ORDER BY totalunits DESC
""", conn)


# STEP 8
# --------------------------------------------------
# Determine market reach per product.
# COUNT(DISTINCT ...) ensures each customer is only counted once.
df_total_customers = pd.read_sql("""
    SELECT
        p.productName,
        p.productCode,
        COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od
        ON p.productCode = od.productCode
    JOIN orders o
        ON od.orderNumber = o.orderNumber
    GROUP BY
        p.productName,
        p.productCode
    ORDER BY numpurchasers DESC
""", conn)


# STEP 9
# --------------------------------------------------
# Count how many customers are associated with each office.
# Offices link to employees, employees link to customers.
df_customers = pd.read_sql("""
    SELECT
        o.officeCode,
        o.city,
        COUNT(c.customerNumber) AS n_customers
    FROM offices o
    JOIN employees e
        ON o.officeCode = e.officeCode
    JOIN customers c
        ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY
        o.officeCode,
        o.city
""", conn)


# STEP 10
# --------------------------------------------------
# Identify employees who sold products ordered by fewer than 20 customers.
# A CTE (WITH clause) isolates low-demand products first.
# DISTINCT ensures employees are not duplicated.
# ORDER BY ensures deterministic output for testing.
df_under_20 = pd.read_sql("""
    WITH low_demand_products AS (
        SELECT
            od.productCode
        FROM orderdetails od
        JOIN orders o
            ON od.orderNumber = o.orderNumber
        GROUP BY od.productCode
        HAVING COUNT(DISTINCT o.customerNumber) < 20
    )
    SELECT DISTINCT
        e.employeeNumber,
        e.firstName,
        e.lastName,
        of.city,
        of.officeCode
    FROM employees e
    JOIN offices of
        ON e.officeCode = of.officeCode
    JOIN customers c
        ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders o
        ON c.customerNumber = o.customerNumber
    JOIN orderdetails od
        ON o.orderNumber = od.orderNumber
    WHERE od.productCode IN (
        SELECT productCode FROM low_demand_products
    )
    ORDER BY
        e.lastName,
        e.firstName
""", conn)


# Close the database connection when all queries are complete
conn.close()
