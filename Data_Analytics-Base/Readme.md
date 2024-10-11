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










