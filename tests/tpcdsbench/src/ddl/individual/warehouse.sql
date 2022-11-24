drop table if exists warehouse_text;
create table warehouse_text
(
    w_warehouse_sk            int,
    w_warehouse_id            string,
    w_warehouse_name          string,
    w_warehouse_sq_ft         int,
    w_street_number           string,
    w_street_name             string,
    w_street_type             string,
    w_suite_number            string,
    w_city                    string,
    w_county                  string,
    w_state                   string,
    w_zip                     string,
    w_country                 string,
    w_gmt_offset              double
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/warehouse")
;
drop table if exists warehouse;
create table warehouse
using parquet
as (select * from warehouse_text)
;
drop table if exists warehouse_text;
