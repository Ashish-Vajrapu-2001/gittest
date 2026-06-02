-- Clear existing data for clean insert
DELETE FROM control.silver_transformation_rules;
DELETE FROM control.silver_table_config;

-- Insert Silver Table Configurations
INSERT INTO control.silver_table_config 
(source_system, schema_name, source_bronze_table, target_silver_table, transformation_type, scd_type, is_active)
VALUES 
('SRC-002', 'CRM', 'Customers', 'customers', 'TRANSFORM', 2, 1),
('SRC-001', 'ERP', 'OE_ORDER_HEADERS_ALL', 'orders', 'TRANSFORM', 1, 1),
('SRC-001', 'ERP', 'OE_ORDER_LINES_ALL', 'order_lines', 'TRANSFORM', 1, 1),
('SRC-003', 'MARKETING', 'MARKETING_CAMPAIGNS', 'marketing_campaigns', 'TRANSFORM', 1, 1),
('SRC-002', 'CRM', 'SURVEYS', 'surveys', 'TRANSFORM', 1, 1);

DECLARE @CustomersId INT = (SELECT silver_table_id FROM control.silver_table_config WHERE target_silver_table = 'customers');
DECLARE @OrdersId INT = (SELECT silver_table_id FROM control.silver_table_config WHERE target_silver_table = 'orders');
DECLARE @OrderLinesId INT = (SELECT silver_table_id FROM control.silver_table_config WHERE target_silver_table = 'order_lines');
DECLARE @CampaignsId INT = (SELECT silver_table_id FROM control.silver_table_config WHERE target_silver_table = 'marketing_campaigns');
DECLARE @SurveysId INT = (SELECT silver_table_id FROM control.silver_table_config WHERE target_silver_table = 'surveys');

-- Insert Transformation Rules for silver.customers
INSERT INTO control.silver_transformation_rules (silver_table_id, rule_name, rule_type, source_column, target_column, transformation_expression, execution_sequence)
VALUES 
(@CustomersId, 'Cast Customer ID', 'CAST', 'CUSTOMER_ID', 'customer_id', 'bigint', 10),
(@CustomersId, 'Trim and Lower Email', 'TRANSFORM', 'EMAIL', 'email', 'lower(trim(EMAIL))', 20),
(@CustomersId, 'Trim Phone', 'TRANSFORM', 'PHONE', 'phone', 'trim(PHONE)', 30),
(@CustomersId, 'Trim First Name', 'TRANSFORM', 'FIRST_NAME', 'first_name', 'trim(FIRST_NAME)', 40),
(@CustomersId, 'Trim Last Name', 'TRANSFORM', 'LAST_NAME', 'last_name', 'trim(LAST_NAME)', 50),
(@CustomersId, 'Trim and Upper Gender', 'TRANSFORM', 'GENDER', 'gender', 'upper(trim(GENDER))', 60),
(@CustomersId, 'Cast DOB', 'CAST', 'DATE_OF_BIRTH', 'date_of_birth', 'date', 70),
(@CustomersId, 'Cast Registration Date', 'CAST', 'REGISTRATION_DATE', 'registration_date', 'timestamp', 80),
(@CustomersId, 'Trim and Upper Customer Type', 'TRANSFORM', 'CUSTOMER_TYPE', 'customer_type', 'upper(trim(CUSTOMER_TYPE))', 90),
(@CustomersId, 'Trim and Upper Status', 'TRANSFORM', 'STATUS', 'status', 'upper(trim(STATUS))', 100),
(@CustomersId, 'Cast Email Verified', 'CAST', 'EMAIL_VERIFIED', 'email_verified', 'boolean', 110),
(@CustomersId, 'Cast Phone Verified', 'CAST', 'PHONE_VERIFIED', 'phone_verified', 'boolean', 120),
(@CustomersId, 'Cast Marketing Opt In', 'CAST', 'MARKETING_OPT_IN', 'marketing_opt_in', 'boolean', 130),
(@CustomersId, 'Trim and Upper Preferred Language', 'TRANSFORM', 'PREFERRED_LANGUAGE', 'preferred_language', 'upper(trim(PREFERRED_LANGUAGE))', 140),
(@CustomersId, 'Deduplicate Customers', 'DEDUPE', NULL, NULL, 'customer_id', 999);

-- Insert Transformation Rules for silver.orders
INSERT INTO control.silver_transformation_rules (silver_table_id, rule_name, rule_type, source_column, target_column, transformation_expression, execution_sequence)
VALUES 
(@OrdersId, 'Cast Order ID', 'CAST', 'ORDER_ID', 'order_id', 'bigint', 10),
(@OrdersId, 'Trim Order Number', 'TRANSFORM', 'ORDER_NUMBER', 'order_number', 'trim(ORDER_NUMBER)', 20),
(@OrdersId, 'Cast Customer ID', 'CAST', 'CUSTOMER_ID', 'customer_id', 'bigint', 30),
(@OrdersId, 'Cast Order Date', 'CAST', 'ORDER_DATE', 'order_date', 'timestamp', 40),
(@OrdersId, 'Trim Order Status', 'TRANSFORM', 'ORDER_STATUS', 'order_status', 'trim(ORDER_STATUS)', 50),
(@OrdersId, 'Trim Payment Method', 'TRANSFORM', 'PAYMENT_METHOD', 'payment_method', 'trim(PAYMENT_METHOD)', 60),
(@OrdersId, 'Trim Payment Status', 'TRANSFORM', 'PAYMENT_STATUS', 'payment_status', 'trim(PAYMENT_STATUS)', 70),
(@OrdersId, 'Cast Subtotal Amount', 'CAST', 'SUBTOTAL_AMOUNT', 'subtotal_amount', 'decimal(15,2)', 80),
(@OrdersId, 'Cast and Coalesce Discount Amount', 'TRANSFORM', 'DISCOUNT_AMOUNT', 'discount_amount', 'coalesce(cast(DISCOUNT_AMOUNT as decimal(15,2)), 0)', 90),
(@OrdersId, 'Cast Tax Amount', 'CAST', 'TAX_AMOUNT', 'tax_amount', 'decimal(15,2)', 100),
(@OrdersId, 'Cast Shipping Amount', 'CAST', 'SHIPPING_AMOUNT', 'shipping_amount', 'decimal(15,2)', 110),
(@OrdersId, 'Cast Total Amount', 'CAST', 'TOTAL_AMOUNT', 'total_amount', 'decimal(15,2)', 120),
(@OrdersId, 'Trim and Upper Currency Code', 'TRANSFORM', 'CURRENCY_CODE', 'currency_code', 'upper(trim(CURRENCY_CODE))', 130),
(@OrdersId, 'Cast Shipping Address ID', 'CAST', 'SHIPPING_ADDRESS_ID', 'shipping_address_id', 'bigint', 140),
(@OrdersId, 'Filter Cancelled/Returned', 'FILTER', NULL, NULL, 'order_status NOT IN (''Cancelled'', ''Returned'')', 150),
(@OrdersId, 'Deduplicate Orders', 'DEDUPE', NULL, NULL, 'order_id', 999);

-- Insert Transformation Rules for silver.order_lines
INSERT INTO control.silver_transformation_rules (silver_table_id, rule_name, rule_type, source_column, target_column, transformation_expression, execution_sequence)
VALUES 
(@OrderLinesId, 'Cast Line ID', 'CAST', 'LINE_ID', 'line_id', 'bigint', 10),
(@OrderLinesId, 'Cast Order ID', 'CAST', 'ORDER_ID', 'order_id', 'bigint', 20),
(@OrderLinesId, 'Cast Line Number', 'CAST', 'LINE_NUMBER', 'line_number', 'int', 30),
(@OrderLinesId, 'Cast Product ID', 'CAST', 'PRODUCT_ID', 'product_id', 'bigint', 40),
(@OrderLinesId, 'Trim and Upper SKU', 'TRANSFORM', 'SKU', 'sku', 'upper(trim(SKU))', 50),
(@OrderLinesId, 'Cast Quantity', 'CAST', 'QUANTITY', 'quantity', 'int', 60),
(@OrderLinesId, 'Cast Unit Price', 'CAST', 'UNIT_PRICE', 'unit_price', 'decimal(15,2)', 70),
(@OrderLinesId, 'Cast Line Amount', 'CAST', 'LINE_AMOUNT', 'line_amount', 'decimal(15,2)', 80),
(@OrderLinesId, 'Deduplicate Order Lines', 'DEDUPE', NULL, NULL, 'line_id', 999);

-- Insert Transformation Rules for silver.marketing_campaigns
INSERT INTO control.silver_transformation_rules (silver_table_id, rule_name, rule_type, source_column, target_column, transformation_expression, execution_sequence)
VALUES 
(@CampaignsId, 'Cast Campaign ID', 'CAST', 'CAMPAIGN_ID', 'campaign_id', 'int', 10),
(@CampaignsId, 'Trim Campaign Name', 'TRANSFORM', 'CAMPAIGN_NAME', 'campaign_name', 'trim(CAMPAIGN_NAME)', 20),
(@CampaignsId, 'Trim and Upper Channel', 'TRANSFORM', 'CHANNEL', 'channel', 'upper(trim(CHANNEL))', 30),
(@CampaignsId, 'Cast Start Date', 'CAST', 'START_DATE', 'start_date', 'date', 40),
(@CampaignsId, 'Cast Total Spend', 'CAST', 'TOTAL_SPEND', 'total_spend', 'decimal(15,2)', 50),
(@CampaignsId, 'Cast Customers Acquired', 'CAST', 'CUSTOMERS_ACQUIRED', 'customers_acquired', 'int', 60),
(@CampaignsId, 'Deduplicate Campaigns', 'DEDUPE', NULL, NULL, 'campaign_id', 999);

-- Insert Transformation Rules for silver.surveys
INSERT INTO control.silver_transformation_rules (silver_table_id, rule_name, rule_type, source_column, target_column, transformation_expression, execution_sequence)
VALUES 
(@SurveysId, 'Cast Survey ID', 'CAST', 'SURVEY_ID', 'survey_id', 'bigint', 10),
(@SurveysId, 'Cast Customer ID', 'CAST', 'CUSTOMER_ID', 'customer_id', 'bigint', 20),
(@SurveysId, 'Trim and Upper Survey Type', 'TRANSFORM', 'SURVEY_TYPE', 'survey_type', 'upper(trim(SURVEY_TYPE))', 30),
(@SurveysId, 'Cast NPS Score', 'CAST', 'NPS_SCORE', 'nps_score', 'int', 40),
(@SurveysId, 'Cast CSAT Score', 'CAST', 'CSAT_SCORE', 'csat_score', 'int', 50),
(@SurveysId, 'Cast Response Date', 'CAST', 'RESPONSE_DATE', 'response_date', 'timestamp', 60),
(@SurveysId, 'Deduplicate Surveys', 'DEDUPE', NULL, NULL, 'survey_id', 999);
GO