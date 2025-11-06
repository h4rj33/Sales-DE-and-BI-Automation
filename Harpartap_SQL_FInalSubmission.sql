/* =========================================================
   1. Building the base company hierarchy + sales rollup views
   ========================================================= */


CREATE OR REPLACE VIEW harpartap_company_hierarchy AS
WITH base AS (
  SELECT DISTINCT company
  FROM (
      SELECT company FROM harpartap_header
      UNION SELECT company FROM harpartap_customer
      UNION SELECT company FROM harpartap_salesrep
      UNION SELECT company FROM harpartap_item
      UNION SELECT company FROM harpartap_line
  ) u
  WHERE company IS NOT NULL AND length(company) > 0
)
SELECT
    company,
    CASE
        WHEN company ~ '(USA)$' THEN substr(company,1,length(company)-3)      -- Logic to handle entity-company-region hierarchy
        WHEN company ~ '(CA)$'  THEN substr(company,1,length(company)-2)
        ELSE company
    END::text AS entity,  
    CASE
        WHEN company ~ '(USA)$' THEN 'USA'
        WHEN company ~ '(CA)$'  THEN 'CA'
        ELSE 'OTHER'
    END::text AS region
FROM base;




CREATE OR REPLACE VIEW harpartap_sales_rollup as           --- Sales Rollup View
SELECT
    ch.entity,
    ch.region,
    l.company,
    c.custnum,
    c.salesrepcode,
    sr.name AS salesrep_name,
    l.ordernum,
    l.orderline,
    l.partnum,
    l.price::numeric  AS price,
    l.orderqty::numeric AS orderqty,
    (l.price::numeric * l.orderqty::numeric) AS line_revenue
FROM harpartap_line l
JOIN harpartap_header h
  ON l.ordernum = h.ordernum AND l.company = h.company
JOIN harpartap_customer c
  ON h.custnum = c.custnum AND h.company = c.company
LEFT JOIN harpartap_salesrep sr
  ON c.salesrepcode = sr.salesrepcode AND c.company = sr.company
JOIN harpartap_company_hierarchy ch
  ON l.company = ch.company;


/* =========================================================
   2. Simple exploratory rollups (entity / region / company / rep)
   ========================================================= */

SELECT entity, SUM(line_revenue) AS total_revenue
FROM harpartap_sales_rollup
GROUP BY entity
ORDER BY total_revenue DESC;

SELECT region, SUM(line_revenue) AS total_revenue
FROM harpartap_sales_rollup
GROUP BY region
ORDER BY total_revenue DESC;

SELECT company, SUM(line_revenue) AS total_revenue
FROM harpartap_sales_rollup
GROUP BY company
ORDER BY total_revenue DESC;

SELECT salesrep_name, salesrepcode, company, SUM(line_revenue) AS total_revenue
FROM harpartap_sales_rollup
GROUP BY salesrep_name, salesrepcode, company
ORDER BY total_revenue DESC;


/* =========================================================
   3. Materializing hierarchy over here 
   ========================================================= */

DROP TABLE IF EXISTS harpartap_entity_hierarchy;

CREATE TABLE harpartap_entity_hierarchy AS
SELECT * FROM harpartap_company_hierarchy;



/* =========================================================
   4.Building all the dimension tables now!
   ========================================================= */

DROP TABLE IF EXISTS harpartap_dim_customer;

CREATE TABLE harpartap_dim_customer AS
SELECT
    c.custnum,
    c.custid,
    c.name                                  AS customer_name,
    COALESCE(c.salesrepcode, 'UNMAPPED')    AS salesrepcode,   -- NULL → 'UNMAPPED'
    c.company,
    ch.entity,
    ch.region
FROM harpartap_customer c
JOIN harpartap_company_hierarchy ch
      ON c.company = ch.company;


/* ---------- DIM PRODUCT ---------- */
DROP TABLE IF EXISTS harpartap_dim_product;

CREATE TABLE harpartap_dim_product AS
SELECT
    i.partnum,
    COALESCE(i.product_category, 'UNMAPPED') AS product_category,  -- NULL → 'UNMAPPED'
    i.company,
    ch.entity,
    ch.region
FROM harpartap_item i
JOIN harpartap_company_hierarchy ch
      ON i.company = ch.company;


/* ---------- DIM SALESREP ---------- */
DROP TABLE IF EXISTS harpartap_dim_salesrep;

CREATE TABLE harpartap_dim_salesrep AS
SELECT
    COALESCE(sr.salesrepcode, 'UNMAPPED')   AS salesrepcode,   -- NULL → 'UNMAPPED'
    COALESCE(sr.name,        'UNMAPPED')    AS salesrep_name,  -- NULL → 'UNMAPPED'
    sr.company,
    eh.entity,
    eh.region
FROM harpartap_salesrep sr
LEFT JOIN harpartap_entity_hierarchy eh
      ON sr.company = eh.company;



/* =========================================================
   5. BUILDING THE FACT TABLE
   ========================================================= */

DROP TABLE IF EXISTS harpartap_fct_sales_transaction CASCADE;

CREATE TABLE harpartap_fct_sales_transaction AS
SELECT
    l.ordernum,
    l.orderline,
    l.partnum,
    h.orderdate,
    h.custnum,
    COALESCE(c.salesrepcode, 'UNMAPPED')    AS salesrepcode,   -- NULL → 'UNMAPPED'
    l.company,
    ch.entity,
    ch.region,
    l.price::numeric                        AS price,
    l.orderqty::numeric                     AS orderqty,
    (l.price::numeric * l.orderqty::numeric) AS line_revenue,
    h.cogs::numeric                         AS cogs
FROM harpartap_line l
JOIN harpartap_header h
      ON l.ordernum = h.ordernum AND l.company = h.company
LEFT JOIN harpartap_customer c
      ON h.custnum = c.custnum   AND h.company = c.company
LEFT JOIN harpartap_salesrep sr
      ON c.salesrepcode = sr.salesrepcode AND c.company = sr.company
JOIN harpartap_company_hierarchy ch
      ON l.company = ch.company;


/* =========================================================
   6. Exploratory calculations for gross margin (using final fact table created by me)
   ========================================================= */

-- Monthly GM% (raw COGS direct)
SELECT 
    company,
    DATE_TRUNC('month', orderdate::date) AS revenue_month,
    SUM(line_revenue) AS total_revenue,
    (SUM(line_revenue) - SUM(cogs)) / NULLIF(SUM(line_revenue), 0) * 100 AS gm_percentage
FROM harpartap_fct_sales_transaction
GROUP BY company, DATE_TRUNC('month', orderdate::date)
ORDER BY company, revenue_month;

-- Identify suspicious rows: zero revenue but positive COGS
SELECT COUNT(*)
FROM harpartap_fct_sales_transaction
WHERE line_revenue::numeric = 0
  AND cogs::numeric > 0;

-- Recompute revenue for zero-revenue rows where price & qty exist
UPDATE harpartap_fct_sales_transaction
SET line_revenue = price::numeric * orderqty::numeric
WHERE line_revenue::numeric = 0
  AND cogs::numeric > 0
  AND price IS NOT NULL
  AND orderqty IS NOT NULL;

-- GM% treating zero-revenue rows as contributing no COGS
SELECT 
    company,
    DATE_TRUNC('month', orderdate::date) AS revenue_month,
    SUM(line_revenue) AS total_revenue,
    SUM(
        line_revenue -
        CASE WHEN line_revenue = 0 THEN 0 ELSE cogs::numeric END
    ) / NULLIF(SUM(line_revenue), 0) * 100 AS gm_percentage
FROM harpartap_fct_sales_transaction
GROUP BY company, DATE_TRUNC('month', orderdate::date)
ORDER BY company, revenue_month;


/* =========================================================
   7. Final Gross Margin Calculation Logic 
   ========================================================= */

-- Direct vs adjusted gross margin (illustrative)
SELECT 
    company,
    DATE_TRUNC('month', orderdate::date) AS revenue_month,
    SUM(line_revenue) AS total_revenue,
    SUM(line_revenue - cogs::numeric) / NULLIF(SUM(line_revenue), 0) * 100 AS actual_gm_percentage,
    SUM(line_revenue - (line_revenue * 0.81)) / NULLIF(SUM(line_revenue), 0) * 100 AS adjusted_gm_percentage
FROM harpartap_fct_sales_transaction
GROUP BY company, DATE_TRUNC('month', orderdate::date)
ORDER BY company, revenue_month;

-- Imputed COGS for zero-revenue lines
WITH avg_cost AS (
    SELECT 
        company,
        SUM(cogs::numeric) / NULLIF(SUM(orderqty), 0) AS avg_unit_cost
    FROM harpartap_fct_sales_transaction
    WHERE line_revenue > 0
      AND cogs IS NOT NULL
      AND orderqty > 0
    GROUP BY company
),
revenue_data AS (
    SELECT 
        t.company,
        DATE_TRUNC('month', t.orderdate::date) AS revenue_month,
        SUM(t.line_revenue) AS total_revenue,
        SUM(
            CASE 
                WHEN t.line_revenue > 0 THEN t.cogs::numeric
                WHEN t.line_revenue = 0 AND t.orderqty > 0 THEN t.orderqty * a.avg_unit_cost
                ELSE 0
            END
        ) AS estimated_cogs
    FROM harpartap_fct_sales_transaction t
    LEFT JOIN avg_cost a ON t.company = a.company
    GROUP BY t.company, DATE_TRUNC('month', t.orderdate::date)
)
SELECT 
    company,
    revenue_month,
    total_revenue,
    (total_revenue - estimated_cogs) / NULLIF(total_revenue, 0) * 100 AS gm_percentage
FROM revenue_data
ORDER BY company, revenue_month;

-- GM% using raw COGS directly (sanity check variant)
SELECT 
    company,
    DATE_TRUNC('month', orderdate::date) AS revenue_month,
    SUM(line_revenue) AS total_revenue,
    SUM(orderqty * price) AS total_cost,
    (SUM(line_revenue - (cogs)) / NULLIF(SUM(line_revenue), 0)) * 100 AS gm_percentage
FROM harpartap_fct_sales_transaction
GROUP BY company, DATE_TRUNC('month', orderdate::date)
ORDER BY company, revenue_month;

-- GM% using price * qty as proxy cost (what-if)
SELECT 
    company,
    DATE_TRUNC('month', orderdate::date) AS revenue_month,
    SUM(line_revenue) AS total_revenue,
    SUM(orderqty * price) AS total_cost,
    (SUM(line_revenue - (orderqty * price)) / NULLIF(SUM(line_revenue), 0)) * 100 AS gm_percentage
FROM harpartap_fct_sales_transaction
GROUP BY company, DATE_TRUNC('month', orderdate::date)
ORDER BY company, revenue_month;

-- Gross margin expression that collapses to zero for consistent data (kept for anomaly spotting)
SELECT 
    company,
    DATE_TRUNC('month', orderdate::date) AS revenue_month,
    SUM(line_revenue) AS total_revenue,
    SUM(line_revenue - (orderqty * (line_revenue / NULLIF(orderqty, 0)))) AS gross_margin
FROM harpartap_fct_sales_transaction
GROUP BY company, DATE_TRUNC('month', orderdate::date)
ORDER BY company, revenue_month;


/* =========================================================
   8. QA ---- Making Validation View!!! (Matches the validation table provided to us)
   ========================================================= */



CREATE OR REPLACE VIEW harpartap_validation_view AS
WITH order_caps AS (                          --  cap COGS at order level
    SELECT
        ordernum,
        company,
        orderdate,
        SUM(line_revenue)           AS line_revenue,
        MAX(cogs)                   AS max_cogs_per_order
    FROM harpartap_fct_sales_transaction
    GROUP BY ordernum, company, orderdate
), monthly AS (                              --  roll up to month
    SELECT
        company,
        TO_CHAR(orderdate::date, 'YYYY-MM')  AS revenue_month,
        SUM(line_revenue)                    AS total_revenue,
        ROUND(
            (SUM(line_revenue) - SUM(max_cogs_per_order))
            / NULLIF(SUM(line_revenue), 0) * 100.0,
            3
        ) AS gm_percentage
    FROM order_caps
    GROUP BY company, TO_CHAR(orderdate::date, 'YYYY-MM')
)
SELECT *
FROM monthly
ORDER BY                                     --  custom ordering to see if it matches the table provided 
    CASE company
        WHEN 'grandCA'   THEN 1
        WHEN 'grandusa'  THEN 2
        WHEN 'superusa'  THEN 3
        WHEN 'wolfology' THEN 4
        ELSE 5
    END,
    revenue_month;


SELECT * FROM harpartap_validation_view;     -- to view validation view


--- Hierarchies work in power bi and are well defined.
-- Aggregation totals match expected values.
-- Reconcile test outputs with source sales data.





/* QUERIES RUN TIMES ALSO IMAGE PROVIDED OF LOGS:
 
| Build step (in script order)         | Statement type             | Rows created | Duration (DBeaver “Execution Time”) |
| ------------------------------------ | -------------------------- | ------------ | ----------------------------------- |
| 1. `harpartap_company_hierarchy`     | `CREATE OR REPLACE VIEW`   | —            | **0.183 s**                         |
| 2. `harpartap_sales_rollup`          | `CREATE OR REPLACE VIEW`   | —            | **0.072 s**                         |
| 3. `harpartap_entity_hierarchy`      | `CREATE TABLE … AS SELECT` | 4            | **0.266 s**                         |
| 4. `harpartap_dim_customer`          | `CREATE TABLE … AS SELECT` | 31 798       | **1.622 s**                         |
| 5. `harpartap_dim_product`           | `CREATE TABLE … AS SELECT` | 6 443        | **0.255 s**                         |
| 6. `harpartap_dim_salesrep`          | `CREATE TABLE … AS SELECT` | 308          | **0.086 s**                         |
| 7. `harpartap_fct_sales_transaction` | `CREATE TABLE … AS SELECT` | 232 836      | **2.148 s**                         |
| 8. `harpartap_validation_view`       | `CREATE OR REPLACE VIEW`   | —            | **0.117 s**                         |

*
*
*
*
*
*
*BY Harpartap Singh*/

