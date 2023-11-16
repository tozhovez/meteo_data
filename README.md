
# 2023
## 1 .primary key - 
```uniquely identifies each record in a table. Primary keys must contain UNIQUE values, and cannot contain NULL values. A table can have only ONE primary key; and in the table, this primary key can consist of single or multiple columns```

## 2 . foreign key -

```link data in one table to the data in another table. A foreign key column in a table points to a column with unique values in another table (often the primary key column) to create a way of cross-referencing the two tables```

## 3. **What is DML and DDL?**
              
- `DDL is Data Definition Language which is used to define data structures.`
- `DML is Data Manipulation Language that is used to manipulate data itself.`

## 4. ?????

## 5. 
```select * from table where col2 like ' %xyz% ' ```
**How do you make this SQL query faster?**
  - Limit the Number of Columns Returned SELECT * 
  - Indexing: Ensure that the columns involved in your WHERE clause, especially col2 in this case, are indexed. 
  - Use Full-Text Search
  - Partitioning

## 6. Write Python function to get the (list and number) and return True if (a+b = number) and (a in the list) and (b in the list) else return False. 

- f([1,3,7,3,8,4,3], 15) = True (7+8=15)
- f([3, 4, 5, 9, 2], 20) = False

```
      def f(arr, num):
          data = {}
          for i in arr:
              if num - i in data:
                  return True
              data[i] = data.setdefault(i, 0)
      
          return False
      
      print(f([1,7,4,9,7,8,2], 15))
      print(f([1,7,4,9,7,9,2], 15))
      
      print(f([1,7,4,9,7,9,2], 14))
      print(f([1,7,4,9,1,9,2], 14))
```

## 7. sql query:
      SELECT
            b."name" AS Department, 
            a."name" AS Employeer, 
            a."salary" AS Salary
      FROM (
            SELECT 
                  "department_id", 
                  "name", 
                  "salary", 
                  dense_rank() OVER (PARTITION BY "department_id" ORDER BY "salary" DESC) as "runks"
            FROM "Employee" 
            ) a 
            INNER JOIN "Departments" b on(a."department_id"=b."id")
     WHERE a."runks" <= 3

```
| department | employeer | salary |

| IT	     | Priya   | 80000 |
| IT       | Vikas   | 75000 |
| IT       | Priyna  | 75000 |
| IT       | Anil    | 65000 |
| IT       | Santosh | 65000 |
| Siles    | Rajesh  | 90000 |
| Siles    | Nidhi   | 60000 |
| Siles    | Raman   | 55000 | 

```     
```  DROP TABLE IF EXISTS "Departments";
      DROP SEQUENCE IF EXISTS "Departments_id_seq";
      CREATE SEQUENCE "Departments_id_seq" INCREMENT  MINVALUE  MAXVALUE  CACHE ;
      
      CREATE TABLE "public"."Departments" (
          "id" integer DEFAULT nextval('"Departments_id_seq"') NOT NULL,
          "name" character varying NOT NULL,
          CONSTRAINT "Departments_name" UNIQUE ("name"),
          CONSTRAINT "Departments_pkey" PRIMARY KEY ("id")
      ) WITH (oids = false);
      
      INSERT INTO "Departments" ("id", "name") VALUES
      (1,	'IT'),
      (2,	'Siles');
      
      DROP TABLE IF EXISTS "Employee";
      DROP SEQUENCE IF EXISTS "Employee_id_seq";
      CREATE SEQUENCE "Employee_id_seq" INCREMENT  MINVALUE  MAXVALUE  CACHE ;
      
      CREATE TABLE "public"."Employee" (
          "id" integer DEFAULT nextval('"Employee_id_seq"') NOT NULL,
          "name" character varying NOT NULL,
          "salary" integer NOT NULL,
          "department_id" integer NOT NULL,
          CONSTRAINT "Employee_name" UNIQUE ("name"),
          CONSTRAINT "Employee_pkey" PRIMARY KEY ("id")
      ) WITH (oids = false);
      
      INSERT INTO "Employee" ("id", "name", "salary", "department_id") VALUES
      (2,	'Vikas',	75000,	1),
      (3,	'Nisha',	40000,	1),
      (4,	'Nidhi',	60000,	2),
      (6,	'Priya',	80000,	1),
      (7,	'Mohit',	45000,	1),
      (8,	'Rajesh',	90000,	2),
      (9,	'Raman',	55000,	2),
      (10,	'Santosh',	65000,	1),
      (12,	'Mlohit',	40000,	1),
      (11,	'Priyna',	75000,	1),
      (1,	'Anil',	65000,	1);
      
      ALTER TABLE ONLY "public"."Employee" ADD CONSTRAINT "Employee_department_id_fkey" FOREIGN KEY (department_id) REFERENCES "Departments"(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE;
```
----------------------------------------------------------------------------

# meteo_data
### ```Python exam.pdf```
### ```Meteo_Data_Exam.pdf```

  load_wind_data  update_wind_data avg

  Instructions for Downloading and Installing:
  System Requirements:
    Ubuntu 20.04, python 8.5, docker, ....

  Clone:
    https://github.com/tozhovez/meteo-data.git

  in meteo-data directiory:
  Run:

    make install-requirements

    make run-infra

    make create-database (postgres:12)
      connect to DB postgres://docker:docker@localhost:5333/meteo_data


    make load-wind-data
      extracting and inserting data from meteo-data/archive.zip file


    make update-wind-data
    service run every 300 sec and check directory
        ${HOME}/meteo-data-storage/data
    so 1) change permission ${HOME}/meteo-data-storage/data
        sudo chmod (775 or 777) -R ${HOME}/meteo-data-storage
        copy csv files to ${HOME}/meteo-data-storage/data upsert DB


    make query

    
# Questions-SQL

##  1)

 **Imagine a single column in a table that is populated with either a single digit (0-9) or a single character (a-z, A-Z). 
   Write a SQL query to print ‘Fizz’ for a numeric value or ‘Buzz’ for
alphabetical value for all values in that column.**


Example:

`['d', 'x', 'T', 8, 'a', 9, 6, 2, 'V']`

`...should output:`

`['Buzz', 'Buzz', 'Buzz', 'Fizz', 'Buzz','Fizz', 'Fizz', 'Fizz', 'Buzz']`


        SELECT column, CASE WHEN UPPER(column) = LOWER(column) THEN 'Fizz' ELSE 'Buzz' END AS FizzBuzz FROM table;      


## 2) 


**Given the following table named A:
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
    


       SELECT SUM(CASE WHEN x>0 THEN x ELSE 0 END) sum_a, SUM(CASE WHEN x<0 THEN x ELSE 0 END) sum_b
       FROM A;

## 3)


**`Write a SQL query to get the third highest salary of an employee from employee_table?`**


    
                SELECT salary FROM employee_table ORDER BY salary DESC LIMIT 1 OFFSET 2;





## 4)



**Consider the Employee table below.**


<u>Emp_Id Emp_name Salary Manager_Id</u>
```
| 10 | Anil | 50000 | 18 |
| 11 |  Vikas |  75000 |  16 |
| 12 |  Nisha |  40000 |  18 |
| 13 |  Nidhi |  60000 |  17 |
| 14 |  Priya |  80000 |  18 |
| 15 |  Mohit |  45000 |  18 |
| 16 |  Rajesh |  90000 |  – |
| 17 |  Raman |  55000 |  16 |
| 18 |  Santosh |  65000 |  17 |
```

<u> Write a query to generate below output:</u>
<u>`Manager_Id Manager Average_Salary_Under_Manager`</u>

- 16 Rajesh 65000
- 17 Raman 62500
- 18 Santosh 53750



              
```
      SELECT b.emp_id AS Manager_Id, b.emp_name AS Manager, AVG(a.salary) AS Average_Salary_Under_Manager
                    FROM Employee a, Employee b
                    WHERE a.manager_id = b.emp_id
                    GROUP BY b.emp_id, b.emp_name
                    ORDER BY b.emp_id,

```



## 5)
**What is DML and DDL?**
              
- `DDL is Data Definition Language which is used to define data structures.`
- `DML is Data Manipulation Language that is used to manipulate data itself.`
                
                


## 6)

 **What is the difference between UNION and UNION ALL**

    
                UNION performs a DISTINCT on the result set, eliminating any duplicate rows.
                UNION ALL does not remove duplicates, and it is therefore faster than UNION.



## 7)

**What is the difference between WHERE and HAVING**

> 
                If the query does not use GROUP BY, then the WHERE and HAVING clauses are equivalent.
                But when using GROUP BY: The HAVING clause is used to filter values from a group (that is, to test conditions after aggregation into groups). The WHERE clause is used to filter records from the result before any grouping is triggered.




##  8)
**How to fetch values from TestTable1 that are not in TestTable2 without using NOT keyword?**    

            SELECT values
            FROM TestTable1 a LEFT JOIN TestTable2 b ON (a.value=b.value)
            WHERE b.value IS NULL


## 9)
**What is wrong with this SQL query? Correct it so it executes properly.**


    SELECT Id, YEAR(BillingDate) AS BillingYear
    FROM Invoices
    WHERE BillingYear >= 2010;

    

`Invalid BillingYear expression in WHERE clause. Although it is defined as an alias in the SELECT before WHERE, the logical order of processing conditions is different.`


            SELECT Id, YEAR(BillingDate) AS BillingYear
            FROM Invoices
            WHERE YEAR(BillingDate) >= 2010;

## 10)

**Explain type of joins:**
           
        1.Inner join: returns only those records that match in both the tables.

            SELECT a.*, b.* FROM A a
            INNER JOIN B b
            ON a.key=b.key;

        2.Left outer: join returns all records from left table and only matching records from right.

        3.Right outer: join returns all records from right table and only matching records from left.

        4.Full outer join: combines left outer join and right outer join. This join returns all records/rows from both the tables.

        5.Cross join: is a cartesian join means cartesian product of both the tables. This join does not need any condition to join two tables.

        6.Self join: is used to join a database table to itself

    







