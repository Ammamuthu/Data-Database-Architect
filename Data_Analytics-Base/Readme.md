# Data Analysis Questions

## 1. How do you write a query to find duplicate rows in a table?

```sql
SELECT column1, column2, COUNT(*) AS cnt
FROM your_table
GROUP BY column1, column2
HAVING COUNT(*) > 1;