-- Basic E-T-L Script

CREATE TABLE india_cus (
    transactionid INT PRIMARY KEY,
    date DATE,
    customername TEXT,
    product TEXT,
    quantity INT,
    unitprice NUMERIC,
    country TEXT
);


CREATE TABLE usa_cus (
    transactionid INT PRIMARY KEY,
    date DATE,
    customername TEXT,
    product TEXT,
    quantity INT,
    unitprice NUMERIC,
    country TEXT
);

show tables;

-- Basic E-L-T Script

CREATE TABLE WW_Customer (
    transactionid INT PRIMARY KEY,
    date DATE,
    customername TEXT,
    product TEXT,
    quantity INT,
    unitprice NUMERIC,
    country TEXT
);
