# Data Analysis Questions
![image](https://github.com/user-attachments/assets/7e490712-8f7e-42db-9294-f25858dc7afa)


# Query Execution ORDER

a) FROM: The database first identifies the tables involved in the query and constructs the initial dataset.

b) JOIN: Any joins specified in the query are processed next.

c) WHERE: Filters are applied to the dataset to exclude rows that do not meet the specified conditions.

d) GROUP BY: If specified, the dataset is grouped based on the columns listed.

e) HAVING: This clause filters groups based on aggregate conditions.

f) SELECT: The columns specified in the SELECT clause are determined.

g) ORDER BY: Finally, the result set is sorted based on the specified columns.

h) LIMIT / OFFSET: If applicable, limits the number of rows returned.

## 1. How do you write a query to find duplicate rows in a table?

```sql
SELECT column1, column2, COUNT(*) AS cnt
FROM your_table
GROUP BY column1, column2
HAVING COUNT(*) > 1;
```
## Explanation: Getting the columns from table and Count(\*) no of times it came and use Having if it greater than 1 count(\*) > 1  .

If you want to see all columns of the duplicate rows, you can join this result back to the original table:

```sql
SELECT *
FROM your_table
WHERE (column1, column2) IN (
    SELECT column1, column2
    FROM your_table
    GROUP BY column1, column2
    HAVING COUNT(*) > 1
);

```

This will give you all columns for each row that is considered a duplicate based on the specified columns. Adjust column1, column2, and your_table as needed for your specific case!

<hr>

## 2. - How would you perform a left join and filter out nulls in SQL?

```sql
SELECT e.employee_id, e.employee_name, d.department_name
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id
WHERE d.department_id IS NOT NULL;

```
When you perform a LEFT JOIN, all rows from the left table (in this case, employees) are included, along with matching rows from the right table (departments). If there is no match, the columns from the right table will be filled with NULL.

### Goal
The goal here is to exclude employees who do not belong to any department. To do this, you check for NULL in the department_id from the departments table.
### Clarification
•	WHERE d.department_id IS NOT NULL: This condition filters out any rows where there was no matching department. You only want rows where the employee has a valid department_id.
If you want to include only those employees that do belong to a department, this is the correct condition to use.
If you wanted to see employees without departments, you would use:
WHERE d.department_id IS NULL;
But for your original question (to show only employees who have a department), you want to keep the IS NOT NULL condition.
## Summary
•	Use IS NOT NULL to show employees with departments.
•	Use IS NULL to show employees without departments.
If your requirement was indeed to find employees without departments, then you would switch to using IS NULL.

<hr>

## 3. - What is a window function in SQL, and how do you use it for ranking data?

To rank data using window functions, you typically use functions like ROW_NUMBER(), RANK(), or DENSE_RANK(). Here's a brief overview of each:
1.	ROW_NUMBER(): Assigns a unique sequential integer to rows within a partition of a result set. The numbering resets with each partition.
2.	RANK(): Assigns a rank to each row within a partition, with gaps for ties. For example, if two rows are tied for rank 1, the next rank will be 3.
3.	DENSE_RANK(): Similar to RANK(), but it does not leave gaps between ranks. So, if two rows are tied for rank 1, the next rank will be 2.


| department | employee_name | salary |
|------------|---------------|--------|
| Sales      | Alice         | 70000  |
| Sales      | Bob           | 60000  |
| Sales      | Charlie       | 60000  |
| Sales      | David         | 50000  |
| HR         | Emma          | 80000  |
| HR         | Frank         | 75000  |
| HR         | Grace         | 75000  |
| HR         | Hannah        | 60000  |


```sql
SELECT 
    department,
    employee_name,
    salary,

    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank

FROM 
    employees;

```

Output
| department | employee_name | salary | row_num | rank | dense_rank |
|------------|---------------|--------|---------|------|------------|
| Sales      | Alice         | 70000  | 1       | 1    | 1          |
| Sales      | Bob           | 60000  | 2       | 2    | 2          |
| Sales      | Charlie       | 60000  | 3       | 2    | 2          |
| Sales      | David         | 50000  | 4       | 4    | 3          |
| HR         | Emma          | 80000  | 1       | 1    | 1          |
| HR         | Frank         | 75000  | 2       | 2    | 2          |
| HR         | Grace         | 75000  | 3       | 2    | 2          |
| HR         | Hannah        | 60000  | 4       | 4    | 3          |
						

In this dataset:
1. RANK() leaves gaps in the ranking when there are ties (e.g., 2, 2, 4).
2. DENSE_RANK() does not leave gaps, resulting in sequential ranks (e.g., 2, 2, 3).

<hr>

## 4. - How do you calculate the cumulative sum for a column in SQL?

### What is Cumulative?
"Cumulative" refers to a total that is gradually increasing by the addition of each successive element. In the context of a cumulative sum in SQL, it means that each row's value adds to the total of all previous rows.

### Example with Output
Let’s say you have a table called sales with the following data:

| sale_date  | amount |
|------------|--------|
| 2024-01-01 | 100    |
| 2024-01-02 | 200    |
| 2024-01-03 | 150    |
| 2024-01-04 | 300    |

You want to calculate the cumulative sum of the amount column. The SQL query would be:
```sql
SELECT 
    sale_date,
    amount,
    SUM(amount) OVER (ORDER BY sale_date) AS cumulative_amount
FROM 
    sales;
```
### Expected Output
After running the query, you would get the following result:

| sale_date  | amount | cumulative_amount |
|------------|--------|--------------------|
| 2024-01-01 | 100    | 100                |
| 2024-01-02 | 200    | 300                |
| 2024-01-03 | 150    | 450                |
| 2024-01-04 | 300    | 750                |

### Explanation of Output
•	On 2024-01-01, the cumulative amount is 100 (just the first row).
•	On 2024-01-02, the cumulative amount is 300 (100 from the first row + 200 from the second row).
•	On 2024-01-03, it becomes 450 (300 from the previous rows + 150 from the third row).
•	On 2024-01-04, the total reaches 750 (450 from the previous rows + 300 from the fourth row).

### Normal Sum :
Note :  When you use Normal sum it Give a On ROW but if we use Windows function then only we will get multiple rows

![image](https://github.com/user-attachments/assets/8dd51a5c-4da7-4899-bb0c-d1701becc33e)

<hr>

## 5. - What is the difference between UNION and UNION ALL in SQL?
•	Use UNION when you need distinct results.
•	Use UNION ALL when you want to include all rows, including duplicates, and possibly for better performance.
Union all  is faster than Union Because it no need to remove duplicates .

### Employees Table

| ID | Name    |
|----|---------|
| 1  | Alice   |
| 2  | Bob     |
| 3  | Charlie  |

### Contractors Table

| ID | Name    |
|----|---------|
| 1  | Bob     |
| 2  | David   |
| 3  | Alice   |

### Example Queries
1.	Using UNION (removes duplicates):

```sql
SELECT Name FROM Employees
UNION
SELECT Name FROM Contractors;
```

### Result:
| Name    |
|---------|
| Alice   |
| Bob     |
| Charlie |
| David   |
  
### Note: "Alice" and "Bob" are only listed once, even though they appear in both tables.
2.	Using UNION ALL (includes duplicates):

```sql
SELECT Name FROM Employees
UNION ALL
SELECT Name FROM Contractors;
Result:
```
| Name    |
|---------|
| Alice   |
| Bob     |
| Charlie |
| Bob     |
| David   |
| Alice   |


### Note: "Alice" and "Bob" appear twice because UNION ALL includes all occurrences.
## Summary:
•	UNION gives you a unique list of names.
•	UNION ALL gives you a complete list, including duplicates.

<hr>
<hr>

# Episode - Excel Needs 

## 1.  What is the difference between absolute and relative cell references, and when would you use each?

#### Reference :

https://edu.gcfglobal.org/en/excelformulas/relative-and-absolute-cell-references/1/

Cell References : Normal Usage 
Cell Absolute : Particular Scenario

### References :

When we want to add two cells we just put like 

= A1 + B1    And drag this toward till end . 

It will work and change the Cells according to other Cells , It is Called References.

### Absolute:

When we want to add a Two values but one of a cell value is Fixed now it will be Difficult For that we are using Absolute  Froze the cell value - Reference 

# Example Formulas

| Row | Formula     |
|-----|-------------|
| 1   | `=A1 * C1`  |
| 2   | `=A2 * C1`  |
| 3   | `=A3 * C1`  |



Like this we need but if we drag like previous it won’t work like Reference C1 also change to C2 .. So for that only we use $ – Symbol

```excel
= A1 * $C$1
```  

Now if you Drag below A1,A2.. An will change not C , This is called Absolute .

![image](https://github.com/user-attachments/assets/6f745a8d-7446-4d92-b999-c2a80f1d3d58)

## VLOOKUP

#### Syntax:

```excel
VLOOKUP(lookup_value,table_array,col_index_num,[large_lookup])
```

| Parameter        | Description                                                   |
|------------------|---------------------------------------------------------------|
| `LOOKUP_VALE`    | What you want to lookup                                       |
| `TABLE_ARRAY`    | Where you want to look for it                                 |
| `col_index_num`  | The column number in the range containing the value to return |
| `Range_Lookup`   | Approximate or Exact Match - indicated as `1`/`True` or `0`/`False` |


### STEP – 1:

Select the column as a Key ( Question)   What value you want to lookup 

![image](https://github.com/user-attachments/assets/b3fc29c3-748a-4a18-8b48-20dce6115333)

### STEP – 2:

Select here all Value as a Table_array  these are the values it will check .

![image](https://github.com/user-attachments/assets/8edf365c-c1b5-4532-b020-bc55379cc701)


### STEP – 3

Here all the Row as 1,2,3,4,5,6 and put the number col_index_num  For which number row you want to return a value
 
![image](https://github.com/user-attachments/assets/2e99cef3-975e-4a9a-bf6b-2302132f3dfe)

### STEP – 4

```excel
=VLOOKUP(J18,A9:F26,6,FALSE)
```

![image](https://github.com/user-attachments/assets/611f7761-ebfd-44cc-8ee7-1b035c407ae4)

Whole formula look like above

### Output :

![image](https://github.com/user-attachments/assets/04a58699-d42e-4122-8cce-16ce46495089)

# VLOOKUP Tips

| Tip Number | Description                                                                                       |
|------------|---------------------------------------------------------------------------------------------------|
| 1          | The value you are trying to LOOKUP (key) must be in the first column for VLOOKUP to work.        |
| 2          | When selecting the TABLE_Array, press `F4` after dragging and dropping to lock the values.      |
| 3          | Avoid naming a value (LOOKUP) that is not present in the table.                                 |

## FINAL VLOOKUP SHEET

![image](https://github.com/user-attachments/assets/1b148f08-40b7-4d77-bd7b-3b998231d698)

<hr>

## XLOOKUP

### Syntax:

```excel
XLOOKUP(lookup_value, lookup_array, return_array, [if_not_found], [match_mode], [search_mode])
```

# VLOOKUP Parameters

| Parameter       | Description                                                        |
|------------------|--------------------------------------------------------------------|
| `Lookup_value`   | The value to search for.                                           |
| `Lookup_array`   | The range or array where to search.                                |
| `Return_array`   | The range or array from which to return values.                   |
| `If_not_found`   | [optional] The value to return if no match is found. Defaults to #N/A error if omitted. |
| `Match_mode`     | [optional] The match type to perform.                              |

### Step1:

=XLOOKUP(What is Value that we looking for)  CELL 

 ![image](https://github.com/user-attachments/assets/22637134-bf70-4c37-b223-09b1ab99f1f4)


### Step – 2 :

What are range we have to search as like Key : Value pair(Name column all should be search) – Range Cells

![image](https://github.com/user-attachments/assets/f80bc07b-5785-4579-b614-a7fdb69e8ac8)

 ### Step -3:

What is Value should be need to return ( Total column should be the output Value ) – Range cells

![image](https://github.com/user-attachments/assets/5ff5e144-8bb1-4ac3-a0ea-b1435a561623)

 

#### Got the Output :
 
![image](https://github.com/user-attachments/assets/218422b1-733e-4456-9e27-a092e308a35b)


### Step – 4:

When I drag down I get all output because we Presses f4 for Locked (F4 - Absolute cell reference).
 
![image](https://github.com/user-attachments/assets/0515b6c8-a568-4219-867b-104d80408659)

The Last name is not in the list so for that we need to pass a default value like this .

### Step – 5 :

Syntax : added a [if_not_found],   =XLOOKUP(J14,A11:A30,F11:F30,"No Name Present")

Value should be in a “ “ .

![image](https://github.com/user-attachments/assets/9dc4daa3-5e4c-4a22-bead-2e7dd23f1c12)


Now we get a Output like no name present instead of syntax Error like (#N/A, #NAME).
 
![image](https://github.com/user-attachments/assets/fac901bc-4ab7-4850-98ac-1f183765b9b7)

### Final XLOOKUP SHEET

![image](https://github.com/user-attachments/assets/9e65c642-1d20-48a2-b3b7-e49a1b20d949)

<hr>

## PIVOT Table:

Pivot table is nothing but is used to show a value like grouped,sumerized,re-arrange etc ..

### Go to insert :

![image](https://github.com/user-attachments/assets/03eede37-7973-4a7c-8c73-87cb5c9f8046) 

### Click Pivot Table

![image](https://github.com/user-attachments/assets/9edfb58f-cc52-4ac9-ba5b-804eb5a47d87)

These are all the fields that we have to play with it .

### Output :

This is how data Looks

 ![image](https://github.com/user-attachments/assets/ed713faf-bb00-4811-82e1-4a82c5576f36)

We can make like below output without any query just drag and drop

![image](https://github.com/user-attachments/assets/fdfba84c-419a-489b-8ca3-9470128f3404)

 <hr>

# Excel – Questions

## 1. How would you use VLOOKUP or XLOOKUP to merge data between two Excel sheets?

**Sheet1:** Contains **Salesperson** and **TotalSales**.  
**Sheet2:** Contains **Salesperson** and **Region**.  

On **Sheet2**, in the third column, run the following commands:

### VLOOKUP

```excel
=VLOOKUP(A2, Sheet2!A:B, 2, FALSE)
```


### XLOOKUP

```excel
=VLOOKUP(A2, Sheet2!A:B, 2, FALSE)
```
## Types of Data Analysis with Pivot Tables
1.  Summarization:

Quickly aggregate data to find totals, averages, counts, and other statistical measures.

2. Comparative Analysis:

Compare different categories (e.g., sales by region or product).
 
3. Trend Analysis:

Analyze trends over time by adding date fields to rows or columns and grouping them (e.g., by month or year).

4. Data Filtering:

Use filters to focus on specific data segments, making it easier to analyze subsets of your data.

5. Cross-tabulation:

Create a matrix view to show relationships between two categorical variables.

6. What-If Analysis:

Modify the data in your pivot table to see how changes affect outcomes.

7. Visualization:

Many spreadsheet tools allow you to create charts from pivot table data for visual analysis.

## 2.  How would you use conditional formatting to highlight cells that meet certain criteria?

### Reference:

[How to use Conditional Formating in EXCEL](https://youtu.be/XHT4paRaY4g?si=BmAYMNVOVR2lv1uC)


Question -  more then 50000 format the colum
![image](https://github.com/user-attachments/assets/8b9e3203-bc92-43ac-875d-3225d9c26e08)


## Step – 1:

Home  > Ribbon > Conditional Formatting > New Rule

![image](https://github.com/user-attachments/assets/b3c07b83-3289-443f-ae57-682fa310dc8f)

 
### Step – 2:

![image](https://github.com/user-attachments/assets/3693fc3b-0b07-4c04-bcf0-0f21de481540)


### Step – 3:

![image](https://github.com/user-attachments/assets/bfc82c45-afd8-4f7d-b1be-3fa98a7a4bd8)

 
### Step – 4:
 ![image](https://github.com/user-attachments/assets/1db6ee79-6cd0-4b22-99b7-f6549cfb9cee)

### Done

3 . How do you use the IF, AND, and OR functions together to create complex logical tests?

## Reference:

[How to Use IF,AND and OR - With Multiple Ways](https://youtu.be/MMLGuZg3sI8?si=9JTG71miIwrFEuER)




