# Gold Layer Data Model (Star Schema)

## Overview
The Gold layer implements an Enterprise Data Warehouse Bus Architecture utilizing a Star Schema. It is optimized for calculating Customer Lifetime Value (CLV) and its underlying components.

## Dimensions

### 1. dim_customer (SCD Type 2)
- **Grain:** One row per customer version.
- **Business Key:** `customer_id`
- **Tracked Attributes:** `customer_type`, `status`, `marketing_opt_in`
- **Metadata:** `_valid_from`, `_valid_to`, `_is_current`

### 2. dim_product (SCD Type 1)
- **Grain:** One row per product.
- **Business Key:** `product_id`
- **Hierarchies:** Brand -> Category -> Product

### 3. dim_geography (SCD Type 1)
- **Grain:** One row per address.
- **Business Key:** `address_id`
- **Attributes:** `city`, `state`, `country`, `city_tier`

### 4. dim_campaign (SCD Type 1)
- **Grain:** One row per campaign.
- **Business Key:** `campaign_id`

### 5. dim_date
- **Grain:** One row per day.
- **Business Key:** `dim_date_key` (YYYYMMDD)

## Facts

### 1. fact_orders
- **Grain:** One row per completed order header.
- **Measures:** `subtotal_amount`, `discount_amount`, `tax_amount`, `shipping_amount`, `total_amount`
- **Dimensions:** Date, Customer, Geography

### 2. fact_order_lines
- **Grain:** One row per order line item.
- **Measures:** `quantity`, `line_amount`, `unit_price`
- **Dimensions:** Date, Customer, Product, Geography

### 3. fact_campaign_performance
- **Grain:** One row per campaign snapshot.
- **Measures:** `total_spend`, `customers_acquired`
- **Dimensions:** Date, Campaign

### 4. fact_surveys
- **Grain:** One row per survey response.
- **Measures:** `nps_score`, `csat_score`
- **Dimensions:** Date, Customer

## Aggregates

### 1. agg_customer_clv
- **Grain:** One row per customer.
- **KPIs:** AOV, Purchase Frequency, Lifespan (Months), Allocated CAC, CLV, Loyalty Tier.

### 2. agg_monthly_segment_metrics
- **Grain:** One row per Month + Loyalty Tier + Acquisition Channel.
- **KPIs:** Average CLV, Average AOV, Average CAC, Total Revenue.