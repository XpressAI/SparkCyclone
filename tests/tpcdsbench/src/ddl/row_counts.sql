------------------------------------------------------------------------------
-- Licensed Materials - Property of IBM
--
-- (C) COPYRIGHT International Business Machines Corp. 2017
-- All Rights Reserved.
--
-- US Government Users Restricted Rights - Use, duplication or
-- disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
------------------------------------------------------------------------------

USE ${TPCDS_DBNAME};

select count(*) from call_center;
select count(*) from catalog_page;
select count(*) from catalog_returns;
select count(*) from catalog_sales;
select count(*) from customer;
select count(*) from customer_address;
select count(*) from customer_demographics;
select count(*) from date_dim;
select count(*) from household_demographics;
select count(*) from income_band;
select count(*) from inventory;
select count(*) from item;
select count(*) from promotion;
select count(*) from reason;
select count(*) from ship_mode;
select count(*) from store;
select count(*) from store_returns;
select count(*) from store_sales;
select count(*) from time_dim;
select count(*) from warehouse;
select count(*) from web_page;
select count(*) from web_returns;
select count(*) from web_sales;
select count(*) from web_site;
