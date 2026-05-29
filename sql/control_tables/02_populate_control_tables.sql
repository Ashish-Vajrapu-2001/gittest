-- Clear existing data for clean insert
DELETE FROM control.load_dependencies;
DELETE FROM control.data_quality_rules;
DELETE FROM control.pipeline_execution_log;
DELETE FROM control.table_metadata;
DELETE FROM control.source_systems;
DBCC CHECKIDENT ('control.source_systems', RESEED, 0);
DBCC CHECKIDENT ('control.table_metadata', RESEED, 0);

-- 1. Insert Source Systems
INSERT INTO control.source_systems (source_system_name, source_system_code, source_system_type, is_active)
VALUES 
('Azure SQL ERP', 'SRC-001', 'Azure SQL Database', 1),
('Azure SQL CRM', 'SRC-002', 'Azure SQL Database', 1),
('Azure SQL Marketing', 'SRC-003', 'Azure SQL Database', 1);

-- 2. Insert Table Metadata
DECLARE @SRC_ERP INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_code = 'SRC-001');
DECLARE @SRC_CRM INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_code = 'SRC-002');
DECLARE @SRC_MKT INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_code = 'SRC-003');

INSERT INTO control.table_metadata 
(source_system_id, schema_name, table_name, primary_key_columns, load_type, is_active, initial_load_completed, bronze_path, silver_path, load_priority)
VALUES
-- CRM Tables (SRC-002)
(@SRC_CRM, 'CRM', 'Customers', 'CUSTOMER_ID', 'CDC', 1, 0, 'bronze/SRC-002/CRM/Customers/', 'silver/SRC-002/CRM/Customers/', 10),
(@SRC_CRM, 'CRM', 'CustomerRegistrationSource', 'REGISTRATION_SOURCE_ID', 'WATERMARK', 1, 0, 'bronze/SRC-002/CRM/CustomerRegistrationSource/', 'silver/SRC-002/CRM/CustomerRegistrationSource/', 20),
(@SRC_CRM, 'CRM', 'INCIDENTS', 'INCIDENT_ID', 'CDC', 1, 0, 'bronze/SRC-002/CRM/INCIDENTS/', 'silver/SRC-002/CRM/INCIDENTS/', 50),
(@SRC_CRM, 'CRM', 'INTERACTIONS', 'INTERACTION_ID', 'CDC', 1, 0, 'bronze/SRC-002/CRM/INTERACTIONS/', 'silver/SRC-002/CRM/INTERACTIONS/', 60),
(@SRC_CRM, 'CRM', 'SURVEYS', 'SURVEY_ID', 'CDC', 1, 0, 'bronze/SRC-002/CRM/SURVEYS/', 'silver/SRC-002/CRM/SURVEYS/', 70),

-- ERP Tables (SRC-001)
(@SRC_ERP, 'ERP', 'CITY_TIER_MASTER', 'CITY,STATE', 'WATERMARK', 1, 0, 'bronze/SRC-001/ERP/CITY_TIER_MASTER/', 'silver/SRC-001/ERP/CITY_TIER_MASTER/', 10),
(@SRC_ERP, 'ERP', 'CATEGORIES', 'CATEGORY_ID', 'WATERMARK', 1, 0, 'bronze/SRC-001/ERP/CATEGORIES/', 'silver/SRC-001/ERP/CATEGORIES/', 10),
(@SRC_ERP, 'ERP', 'BRANDS', 'BRAND_ID', 'WATERMARK', 1, 0, 'bronze/SRC-001/ERP/BRANDS/', 'silver/SRC-001/ERP/BRANDS/', 10),
(@SRC_ERP, 'ERP', 'ADDRESSES', 'ADDRESS_ID', 'CDC', 1, 0, 'bronze/SRC-001/ERP/ADDRESSES/', 'silver/SRC-001/ERP/ADDRESSES/', 15),
(@SRC_ERP, 'ERP', 'MTL_SYSTEM_ITEMS_B', 'INVENTORY_ITEM_ID', 'WATERMARK', 1, 0, 'bronze/SRC-001/ERP/MTL_SYSTEM_ITEMS_B/', 'silver/SRC-001/ERP/MTL_SYSTEM_ITEMS_B/', 20),
(@SRC_ERP, 'ERP', 'OE_ORDER_HEADERS_ALL', 'ORDER_ID', 'CDC', 1, 0, 'bronze/SRC-001/ERP/OE_ORDER_HEADERS_ALL/', 'silver/SRC-001/ERP/OE_ORDER_HEADERS_ALL/', 30),
(@SRC_ERP, 'ERP', 'OE_ORDER_LINES_ALL', 'LINE_ID', 'CDC', 1, 0, 'bronze/SRC-001/ERP/OE_ORDER_LINES_ALL/', 'silver/SRC-001/ERP/OE_ORDER_LINES_ALL/', 40),

-- Marketing Tables (SRC-003)
(@SRC_MKT, 'MARKETING', 'MARKETING_CAMPAIGNS', 'CAMPAIGN_ID', 'WATERMARK', 1, 0, 'bronze/SRC-003/MARKETING/MARKETING_CAMPAIGNS/', 'silver/SRC-003/MARKETING/MARKETING_CAMPAIGNS/', 10);

-- 3. Insert Load Dependencies
INSERT INTO control.load_dependencies (table_id, depends_on_table_id, dependency_type)
SELECT t1.table_id, t2.table_id, 'FK'
FROM control.table_metadata t1
JOIN control.table_metadata t2 ON t2.table_name = 'Customers'
WHERE t1.table_name IN ('CustomerRegistrationSource', 'INCIDENTS', 'SURVEYS', 'ADDRESSES', 'OE_ORDER_HEADERS_ALL');

INSERT INTO control.load_dependencies (table_id, depends_on_table_id, dependency_type)
SELECT t1.table_id, t2.table_id, 'FK'
FROM control.table_metadata t1
JOIN control.table_metadata t2 ON t2.table_name = 'OE_ORDER_HEADERS_ALL'
WHERE t1.table_name = 'OE_ORDER_LINES_ALL';

INSERT INTO control.load_dependencies (table_id, depends_on_table_id, dependency_type)
SELECT t1.table_id, t2.table_id, 'FK'
FROM control.table_metadata t1
JOIN control.table_metadata t2 ON t2.table_name = 'MTL_SYSTEM_ITEMS_B'
WHERE t1.table_name = 'OE_ORDER_LINES_ALL';
GO