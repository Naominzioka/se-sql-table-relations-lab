# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# employees from boston
df_boston = pd.read_sql("""
                        SELECT firstName, lastName
                        FROM employees
                        JOIN offices
                        USING(officeCode)
                        WHERE city = 'Boston';
                        """, conn)
print(df_boston)

# STEP 2 -offices with zero employees
# #
df_zero_emp = pd.read_sql("""
                          SELECT o.officeCode, o.city,
                          COUNT(e.employeeNumber) AS numEmployees
                          FROM offices o
                          LEFT JOIN employees e
                          USING(officeCode)
                          GROUP BY officeCode
                          HAVING numEmployees = 0;
                          """, conn)
print(df_zero_emp)



# STEP 3 employee details
df_employee = pd.read_sql("""
                          SELECT firstName, lastName, city, state
                          FROM employees
                          JOIN offices
                          USING(officeCode)
                          ORDER BY firstName, lastName
                          """, conn)
print(df_employee)

# STEP 4 contacts for customers who have not placed an order
df_contacts = pd.read_sql("""
                          SELECT c.contactFirstName, c.contactLastName, 
                          c.phone, c.salesRepEmployeeNumber
                          FROM customers AS c
                          LEFT JOIN orders ON orders.customerNumber = c.customerNumber
                          WHERE orderNumber IS NULL
                          ORDER BY c.contactLastName
                          """, conn)
print(df_contacts)

# STEP 5 order customer payments by amount from highest and display cust contacts, payment date
df_payment = pd.read_sql("""
                         SELECT contactFirstName, contactLastName, CAST(amount AS REAL) AS amt, paymentDate
                         FROM customers
                         JOIN payments
                         USING(customerNumber)
                         ORDER BY amt DESC;
                         """, conn)
print("\n------------Customer report----------\n")
print(df_payment)

# STEP 6
df_credit = pd.read_sql("""
                        SELECT employeeNumber, firstName, lastName, COUNT(customerNumber) as totCustomers
                        FROM employees AS e
                        JOIN customers AS c ON c.salesRepEmployeeNumber = e.employeeNumber
                        GROUP BY e.employeeNumber
                        HAVING AVG(c.creditLimit) > 90000
                        ORDER BY totCustomers DESC;
                        """, conn)
print(df_credit)

# STEP 7 - find the most selling products
df_product_sold = pd.read_sql(""" 
                              SELECT p.productName, COUNT(od.quantityOrdered) as numorders, SUM(od.quantityOrdered) as totalunits
                                FROM orderdetails od
                                JOIN products p USING(productCode)
                                GROUP BY od.productCode
                                ORDER BY totalunits DESC;
                              """, conn)
print(df_product_sold)

# STEP 8
df_total_customers = pd.read_sql("""
                                 SELECT p.productName, p.productCode, COUNT(DISTINCT o.customerNumber) AS numpurchasers
                                 FROM products AS p
                                 JOIN orderDetails AS od ON p.productCode = od.productCode
                                 JOIN orders AS o ON od.orderNumber = o.orderNumber
                                 GROUP BY od.productCode
                                 ORDER BY numpurchasers DESC;
                                 """, conn)
print(df_total_customers)

# STEP 9
df_customers = pd.read_sql("""
                           SELECT o.officeCode, o.city, COUNT(customerNumber) AS n_customers
                           FROM offices o
                           JOIN customers c
                           ON e.employeeNumber = c.salesRepEmployeeNumber
                           JOIN employees e
                           USING(officeCode)
                           GROUP BY officeCode
                           """, conn)
print(df_customers)

# STEP 10
# df_under_20 = pd.read_sql("""
#                           SELECT p.productName, p.productCode, COUNT(DISTINCT o.customerNumber) as numpurchasers
#                           FROM products AS p
#                             JOIN orderDetails AS od ON p.productCode = od.productCode
#                             JOIN orders AS o ON od.orderNumber = o.orderNumber
#                           GROUP BY p.productCode
#                           HAVING numpurchasers < 20
#                           """, conn)
# print(df_under_20)



df_under_20 = pd.read_sql("""WITH RareProducts AS (
        SELECT od.productCode
        FROM orderDetails od
        JOIN orders o ON od.orderNumber = o.orderNumber
        GROUP BY od.productCode
        HAVING COUNT(DISTINCT o.customerNumber) < 20
    )
    SELECT DISTINCT 
        e.employeeNumber, 
        e.firstName, 
        e.lastName, 
        off.city, 
        off.officeCode
    FROM employees e
    JOIN offices off ON e.officeCode = off.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders o ON c.customerNumber = o.customerNumber
    JOIN orderDetails od ON o.orderNumber = od.orderNumber
    JOIN RareProducts rp ON od.productCode = rp.productCode
    ORDER by e.lastName;
    """, conn)
print(df_under_20)
conn.close()