#  1)

 **Imagine a single column in a table that is populated with either a single digit (0-9) or a single character (a-z, A-Z). 
   Write a SQL query to print ‘Fizz’ for a numeric value or ‘Buzz’ for
alphabetical value for all values in that column.**


Example:

`['d', 'x', 'T', 8, 'a', 9, 6, 2, 'V']`

`...should output:`

`['Buzz', 'Buzz', 'Buzz', 'Fizz', 'Buzz','Fizz', 'Fizz', 'Fizz', 'Buzz']`


        SELECT column, CASE WHEN UPPER(column) = LOWER(column) THEN 'Fizz' ELSE 'Buzz' END AS FizzBuzz FROM table;      


# 2) 


** Given the following table named A:
Write a single query to calculate the sum of all positive values of x and the sum of all negative
values of x**

        |  x    |
        |-------|
        | 2     |
        |-2     |
        | 4     |
        |-4     |
        |-3     |
        | 0     |
        | 2     |
    


`       SELECT SUM(CASE WHEN x>0 THEN x ELSE 0 END) sum_a, SUM(CASE WHEN x<0 THEN x ELSE 0 END) sum_b
            FROM A;`

# 3)


**
   `Write a SQL query to get the third highest salary of an employee from employee_table?
`**


    
                SELECT salary FROM employee_table ORDER BY salary DESC LIMIT 1 OFFSET 2;





# 4)



**Consider the Employee table below.**


<u>Emp_Id Emp_name Salary Manager_Id</u>

* 10 Anil 50000 18
* 11 Vikas 75000 16
* 12 Nisha 40000 18
* 13 Nidhi 60000 17
* 14 Priya 80000 18
* 15 Mohit 45000 18
* 16 Rajesh 90000 –
* 17 Raman 55000 16
* 18 Santosh 65000 17


><u> Write a query to generate below output:</u>
<u>`Manager_Id Manager Average_Salary_Under_Manager`</u>

* 16 Rajesh 65000
* 17 Raman 62500
* 18 Santosh 53750*

# 

              

      SELECT b.emp_id AS Manager_Id, b.emp_name AS Manager, AVG(a.salary) AS Average_Salary_Under_Manager
                    FROM Employee a, Employee b
                    WHERE a.manager_id = b.emp_id
                    GROUP BY b.emp_id, b.emp_name
                    ORDER BY b.emp_id,





# 5)
** What is DML and DDL?
**
              
 ` DDL is Data Definition Language which is used to define data structures.
                DML is Data Manipulation Language which is used to manipulate data itself.`
                
                


# 6)

 **What is the difference between UNION and UNION ALL**

    
                UNION performs a DISTINCT on the result set, eliminating any duplicate rows.
                UNION ALL does not remove duplicates, and it therefore faster than UNION.



# 7)

** What is the difference between WHERE and HAVING**

> 
                If the query does not use GROUP BY, then the WHERE and HAVING clauses are equivalent.
                But when using GROUP BY: The HAVING clause is used to filter values from a group (that is, to test conditions after aggregation into groups).The WHERE clause is used to filter records from the result before any grouping is triggered.




#  8)
** How to fetch values from TestTable1 that are not in TestTable2 without using NOT keyword?
**    

            SELECT values
            FROM TestTable1 a LEFT JOIN TestTable2 b ON (a.value=b.value)
            WHERE b.value IS NULL


# 9)
** What is wrong with this SQL query? Correct it so it executes properly.**


`   SELECT Id, YEAR(BillingDate) AS BillingYear
    FROM Invoices
    WHERE BillingYear >= 2010;
`
    

Invalid BillingYear expression in WHERE clause. Although it is defined as an alias in the SELECT before WHERE, the logical order of processing conditions is different.`


            SELECT Id, YEAR(BillingDate) AS BillingYear
            FROM Invoices
            WHERE YEAR(BillingDate) >= 2010;

# 10)

**Explain type of joins:
**
           
        1.Inner join: returns only those records that match in both the tables.

            SELECT a.*, b.* FROM A a
            INNER JOIN B b
            ON a.key=b.key;

        2.Left outer: join returns all records from left table and only matching records from right.

        3.Right outer: join returns all records from right table and only matching records from left.

        4.Full outer join: combines left outer join and right outer join. This join returns all records/rows from both the tables.

        5.Cross join: is a cartesian join means cartesian product of both the tables. This join does not need any condition to join two tables.

        6.Self join: is used to join a database table to itself

    
