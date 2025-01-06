-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE gold_view AS
SELECT 
    o.customer_id, 
    c.customer_name, 
    o.order_id, 
    o.product_id, 
    o.quantity, 
    o.price, 
    p.brand
FROM 
    STREAM(LIVE.Orders_silver) o
INNER JOIN 
    STREAM(LIVE.customers_silver) c ON o.customer_id = c.customer_id
INNER JOIN 
    STREAM(LIVE.Products_silver) p ON o.product_id = p.product_id
    WHERE o.order_status != 'Pending'

