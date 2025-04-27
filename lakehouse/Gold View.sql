-- Databricks notebook source
CREATE OR REFRESH MATERIALIZED VIEW gold_view AS
SELECT 
    o.customer_id, 
    c.customer_name, 
    o.order_id, 
    o.product_id, 
    o.quantity, 
    o.price, 
    p.brand
FROM 
    LIVE.Orders_silver o
INNER JOIN 
    LIVE.customers_silver c ON o.customer_id = c.customer_id
INNER JOIN 
    LIVE.Products_silver p ON o.product_id = p.product_id
    WHERE o.order_status != 'Pending'


-- COMMAND ----------


