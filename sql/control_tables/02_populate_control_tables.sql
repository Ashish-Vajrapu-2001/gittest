-- 1. Populate Source Systems
INSERT INTO [control].[source_systems] ([source_system_name], [source_system_type], [is_active])
VALUES 
('SRC-001', 'Azure SQL ERP', 1),
('SRC-002', 'Azure SQL CRM', 1),
('SRC-003', 'Azure SQL Marketing', 1);
GO

-- 2. Populate Table Metadata
DECLARE @SRC_001 INT = (SELECT source_system_id FROM [control].[source_systems] WHERE source_system_name = 'SRC-001');
DECLARE @SRC_002 INT = (SELECT source_system_id FROM [control].[source_systems] WHERE source_system_name = 'SRC-002');
DECLARE @SRC_003 INT = (SELECT source_system_id FROM [control].[source_systems] WHERE source_system_name = 'SRC-003');

INSERT INTO [control].[table_metadata] 
([source_system_id], [schema_name], [table_name], [primary_key_columns], [load_type], [is_active], [initial_load_completed], [bronze_path], [silver_path], [load_priority])
VALUES
-- CRM Tables (SRC-002)
(@SRC_002, 'CRM', 'Customers', 'CUSTOMER_ID', 'CDC', 1, 0, 'bronze/SRC-002/CRM/Customers/', 'silver/SRC-002/CRM/Customers/', 10),
(@SRC_002, 'CRM', 'CustomerRegistrationSource', 'REGISTRATION_SOURCE_ID', 'WATERMARK', 1, 0, 'bronze/SRC-002/CRM/CustomerRegistrationSource/', 'silver/SRC-002/CRM/CustomerRegistrationSource/', 20),
(@SRC_002, 'CRM', 'INCIDENTS', 'INCIDENT_ID', 'CDC', 1, 0, 'bronze/SRC-002/CRM/INCIDENTS/', 'silver/SRC-002/CRM/INCIDENTS/', 110),
(@SRC_002, 'CRM', 'INTERACTIONS', 'INTERACTION_ID', 'CDC', 1, 0, 'bronze/SRC-002/CRM/INTERACTIONS/', 'silver/SRC-002/CRM/INTERACTIONS/', 120),
(@SRC_002, 'CRM', 'SURVEYS', 'SURVEY_ID', 'WATERMARK', 1, 0, 'bronze/SRC-002/CRM/SURVEYS/', 'silver/SRC-002/CRM/SURVEYS/', 130),

-- ERP Tables (SRC-001)
(@SRC_001, 'ERP', 'OE_ORDER_HEADERS_ALL', 'ORDER_ID', 'CDC', 1, 0, 'bronze/SRC-001/ERP/OE_ORDER_HEADERS_ALL/', 'silver/SRC-001/ERP/OE_ORDER_HEADERS_ALL/', 30),
(@SRC_001, 'ERP', 'OE_ORDER_LINES_ALL', 'LINE_ID', 'CDC', 1, 0, 'bronze/SRC-001/ERP/OE_ORDER_LINES_ALL/', 'silver/SRC-001/ERP/OE_ORDER_LINES_ALL/', 40),
(@SRC_001, 'ERP', 'ADDRESSES', 'ADDRESS_ID', 'CDC', 1, 0, 'bronze/SRC-001/ERP/ADDRESSES/', 'silver/SRC-001/ERP/ADDRESSES/', 50),
(@SRC_001, 'ERP', 'CITY_TIER_MASTER', 'CITY,STATE', 'WATERMARK', 1, 0, 'bronze/SRC-001/ERP/CITY_TIER_MASTER/', 'silver/SRC-001/ERP/CITY_TIER_MASTER/', 60),
(@SRC_001, 'ERP', 'MTL_SYSTEM_ITEMS_B', 'INVENTORY_ITEM_ID', 'WATERMARK', 1, 0, 'bronze/SRC-001/ERP/MTL_SYSTEM_ITEMS_B/', 'silver/SRC-001/ERP/MTL_SYSTEM_ITEMS_B/', 70),
(@SRC_001, 'ERP', 'CATEGORIES', 'CATEGORY_ID', 'WATERMARK', 1, 0, 'bronze/SRC-001/ERP/CATEGORIES/', 'silver/SRC-001/ERP/CATEGORIES/', 80),
(@SRC_001, 'ERP', 'BRANDS', 'BRAND_ID', 'WATERMARK', 1, 0, 'bronze/SRC-001/ERP/BRANDS/', 'silver/SRC-001/ERP/BRANDS/', 90),

-- Marketing Tables (SRC-003)
(@SRC_003, 'MARKETING', 'MARKETING_CAMPAIGNS', 'CAMPAIGN_ID', 'WATERMARK', 1, 0, 'bronze/SRC-003/MARKETING/MARKETING_CAMPAIGNS/', 'silver/SRC-003/MARKETING/MARKETING_CAMPAIGNS/', 100);
GO

-- 3. Populate Load Dependencies
DECLARE @T_Customers INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'Customers');
DECLARE @T_CustomerReg INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'CustomerRegistrationSource');
DECLARE @T_OrderHeaders INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'OE_ORDER_HEADERS_ALL');
DECLARE @T_OrderLines INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'OE_ORDER_LINES_ALL');
DECLARE @T_Addresses INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'ADDRESSES');
DECLARE @T_CityTier INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'CITY_TIER_MASTER');
DECLARE @T_Items INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'MTL_SYSTEM_ITEMS_B');
DECLARE @T_Categories INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'CATEGORIES');
DECLARE @T_Brands INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'BRANDS');
DECLARE @T_Campaigns INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'MARKETING_CAMPAIGNS');
DECLARE @T_Incidents INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'INCIDENTS');
DECLARE @T_Interactions INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'INTERACTIONS');
DECLARE @T_Surveys INT = (SELECT table_id FROM [control].[table_metadata] WHERE table_name = 'SURVEYS');

INSERT INTO [control].[load_dependencies] ([table_id], [depends_on_table_id], [dependency_type])
VALUES
(@T_OrderHeaders, @T_Customers, 'FK'),
(@T_OrderHeaders, @T_Addresses, 'FK'),
(@T_OrderLines, @T_OrderHeaders, 'FK'),
(@T_OrderLines, @T_Items, 'FK'),
(@T_CustomerReg, @T_Customers, 'FK'),
(@T_CustomerReg, @T_Campaigns, 'FK'),
(@T_Incidents, @T_Customers, 'FK'),
(@T_Incidents, @T_OrderHeaders, 'FK'),
(@T_Interactions, @T_Incidents, 'FK'),
(@T_Interactions, @T_Customers, 'FK'),
(@T_Surveys, @T_Customers, 'FK'),
(@T_Surveys, @T_OrderHeaders, 'FK'),
(@T_Surveys, @T_Incidents, 'FK'),
(@T_Addresses, @T_Customers, 'FK'),
(@T_Addresses, @T_CityTier, 'FK'),
(@T_Items, @T_Categories, 'FK'),
(@T_Items, @T_Brands, 'FK');
GO