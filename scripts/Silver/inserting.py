import pyodbc
from datetime import datetime


'''
This Script will insert data into the silver layer tables
'''


# --- Connection details ---
server = r'DESKTOP-5EU8KIE\SQLEXPRESS'      # e.g. r'DESKTOP-12345\\SQLEXPRESS'
database = 'DataWarehouse'  # e.g. 'mydb'


#Calculation of Execution time
now = datetime.now()
current_time = now.strftime("%S")

# --- Create connection ---
conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    f'SERVER={server};DATABASE={database};')

# --- Create cursor ---
cursor = conn.cursor()

cursor.execute(r""" 
BEGIN TRANSACTION;
--DELETING OLD DATA TO AVOID DUPLICATION
--No duplication or Nulls in pk, tested using group by and count(*)
TRUNCATE TABLE Silver.crm_cust_info;
TRUNCATE TABLE Silver.crm_sales_details;
TRUNCATE TABLE Silver.erp_CUST_AZ12;
TRUNCATE TABLE Silver.erp_LOC_A101;
TRUNCATE TABLE Silver.erp_PX_CAT_G1V2;
							 
INSERT INTO Silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
	--DATA CLEANSING
    TRIM(cst_firstname) AS cst_firstname, 
    TRIM(cst_lastname) AS cst_lastname,   
    --DATA NORMALIZATION AND STANDARDIZATION AND HANDLING MISSING VALUES
    CASE cst_material_status
        WHEN 'M' THEN 'Married'
        WHEN 'S' THEN 'Single'
		ELSE 'n/a'
    END AS cst_material_status,
    CASE cst_gndr 
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
	--REMOVES DUPLICATES
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)t
WHERE flag_last = 1;


insert into Silver.crm_prd_info(
	prd_id,
	cat_id,
    prd_key ,
    prd_nm ,
    prd_cost,
    prd_line ,
    prd_start_dt ,
    prd_end_dt
)
select 
prd_id,
--DERIEVED NEW COLUMN
replace(substring(prd_key,1,5),'-','_')cat_id,
substring(prd_key,7,len(prd_key))prd_key,
prd_nm,
--DATA NORMALIZATION AND HANDLING MISSING VALUES
ISNULL(prd_cost,0) prd_cost,
CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'T' THEN 'Touring'
    ELSE 'n/a'
END AS prd_line,
--DATA TYPECASTING
cast(prd_start_dt as date)prd_start_dt,
--DERIVING prd_end_dt USING LEAD FUNCTION
cast(dateadd(day,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as date)prd_end_dt
from bronze.crm_prd_info 

							 
insert into Silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
select 
sls_ord_num,sls_prd_key,sls_cust_id,
--STANDARDIZATION OF DATES
case when sls_order_dt is null then null
	when len(sls_order_dt)<8 then null
	else cast(cast(sls_order_dt as varchar) as date) end sls_order_dt,
cast(cast(sls_ship_dt as varchar) as date) sls_ship_dt,
cast(cast(sls_due_dt as varchar) as date) sls_due_dt,
case when sls_sales <0 or sls_sales is null or sls_sales != abs(sls_price)*sls_quantity then abs(sls_price)*sls_quantity else sls_sales end sls_sales,
sls_quantity,
--CALCULATION OF SALES PRICE IF NULL
CASE 
    WHEN sls_price IS NULL THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
    ELSE sls_price
END AS sls_price
from Bronze.crm_sales_details

							 
insert into Silver.erp_CUST_AZ12(
		CID,BDATE,GEN
)
select
--REMOVING NAS TO MAKE COLUMN USEFULL 
case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) else cid end CID,
--HANDLING INCORRECT BIRTH DATE
case when Bdate>getdate() then null else bdate end BDATE,
--DATA NORMALIZATION
case when gen is null or gen = '' then 'n/a' 
	 when gen='M' then 'Male'
	 when gen='F' then 'Female'
	 else gen end GEN
from bronze.erp_CUST_AZ12


insert into Silver.erp_LOC_A101(
cid,cntry
)
SELECT						 
  REPLACE(cid, '-', '') AS cid,						 
  CASE 
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
  END AS cntry
FROM bronze.erp_loc_a101;

							 
INSERT INTO silver.erp_px_cat_g1v2
(id, cat, subcat, maintenance)
SELECT
  id,
  cat,
  subcat,
  maintenance
FROM bronze.erp_px_cat_g1v2;
COMMIT;
""")


conn.commit()
# --- Close ---
cursor.close()
conn.close()
# printing execution time
now = datetime.now()
print(int(now.strftime("%S"))-int(current_time))
print('Executed')