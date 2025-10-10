-- CREATE A VIEW for India Filter

CREATE VIEW VW_India_Cus as SELECT * FROM WW_Customer where country='India' and unitprice <= 100;

-- multiple quantity * unitprice = unit price in A MAIN TABLE
UPDATE WW_CUSTOMER SET unitprice = quantity * unitprice WHERE country='India' and unitprice <= 100;
UPDATE WW_Customer SET customername = INITCAP(customername);

-- CREATE A VIEW for USA Filter

CREATE TABLE USA_CUS_ELT as select * from WW_CUSTOMER where country = 'USA';

-- Add one column as total price

ALTER TABLE USA_CUS_ELT ADD COLUMN IF NOT EXISTS totalprice NUMERIC;
UPDATE USA_CUS_ELT set totalprice = unitprice * quantity;
