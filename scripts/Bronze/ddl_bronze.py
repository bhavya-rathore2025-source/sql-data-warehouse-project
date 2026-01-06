import pyodbc

from datetime import datetime

'''
This Script will redefine the structure of the bronze layer,create tables
and load new data into those tables
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


#Creating the tables if they do no exists


cursor.execute(r"""
							 
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info ;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
							 
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;

create table bronze.crm_sales_details (
	sls_ord_num  nvarchar(50),
	sls_prd_key  nvarchar(50),
	sls_cust_id  int,
	sls_order_dt int,
	sls_ship_dt  int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
)
							 
IF OBJECT_ID('bronze.erp_CUST_AZ12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_CUST_AZ12;
create table bronze.erp_CUST_AZ12 (
	CID nvarchar(50),
	BDATE date,
	GEN nvarchar(50)
)
							 						 
IF OBJECT_ID('bronze.erp_LOC_A101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_LOC_A101;
create table bronze.erp_LOC_A101 (
	CID nvarchar(50),
	CNTRY nvarchar(50)
)
							 
IF OBJECT_ID('bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_PX_CAT_G1V2;
create table bronze.erp_PX_CAT_G1V2 (
	ID nvarchar(50),
	CAT nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50)
)
""")
conn.commit()

#Loading the data into tabel(truncate and load)
#IMPORTANT: SQL Server BULK INSERT requires an absolute path accessible by the SQL Server service.
#Example absolute path (update as needed):
#BULK INSERT bronze.crm_cust_info FROM 'C:\\Users\\MY PC\\Documents\\git\\sql-data-warehouse-project\\Source\\source_crm\\cust_info.csv'
cursor.execute(r"""
use DataWarehouse
							 
Truncate table bronze.crm_cust_info
BUlK INsert bronze.crm_cust_info FROM 'C:\\Users\\MY PC\\Documents\\git\\sql-data-warehouse-project\\Source\\source_crm\\cust_info.csv'
with (
    FirstRow=2,
    fieldterminator=','
)
Truncate table bronze.crm_prd_info
BUlK INsert bronze.crm_prd_info FROM 'C:\\Users\\MY PC\\Documents\\git\\sql-data-warehouse-project\\Source\\source_crm\\prd_info.csv'
with (
    FirstRow=2,
    fieldterminator=','
)
Truncate table bronze.crm_sales_details
BUlK INsert bronze.crm_sales_details FROM 'C:\\Users\\MY PC\\Documents\\git\\sql-data-warehouse-project\\Source\\source_crm\\sales_details.csv'
with (
    FirstRow=2,
    fieldterminator=','
)
Truncate table bronze.erp_CUST_AZ12
BUlK INsert bronze.erp_CUST_AZ12 FROM 'C:\\Users\\MY PC\\Documents\\git\\sql-data-warehouse-project\\Source\\source_erp\\CUST_AZ12.csv'
with (
    FirstRow=2,
    fieldterminator=','
)
Truncate table bronze.erp_LOC_A101
BUlK INsert bronze.erp_LOC_A101 FROM 'C:\\Users\\MY PC\\Documents\\git\\sql-data-warehouse-project\\Source\\source_erp\\LOC_A101.csv'
with (
    FirstRow=2,
    fieldterminator=','
)
Truncate table bronze.erp_PX_CAT_G1V2
BUlK INsert bronze.erp_PX_CAT_G1V2 FROM 'C:\\Users\\MY PC\\Documents\\git\\sql-data-warehouse-project\\Source\\source_erp\\PX_CAT_G1V2.csv'
with (
    FirstRow=2,
    fieldterminator=','
)
""")
conn.commit()


# --- Close ---
cursor.close()
conn.close()
# printing execution time
now = datetime.now()
print(int(now.strftime("%S"))-int(current_time))

