import pyodbc
from datetime import datetime

'''
This Script will insert data into the silver layer tables
'''


# --- Connection details ---
server = r'DESKTOP-5EU8KIE\SQLEXPRESS'      # e.g. r'DESKTOP-12345\\SQLEXPRESS'
database = 'DataWarehouse'  # e.g. 'mydb'


# --- Create connection ---
conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    f'SERVER={server};DATABASE={database};')

# --- Create cursor ---
cursor = conn.cursor()
cursor.execute("""drop view if exists gold.dm_customers;
drop view if exists gold.fact_sales;
drop view if exists gold.dm_products
							 """)
conn.commit()
# --- Create Views ---
cursor.execute(r"""
create view gold.dm_customers as  
select   
		ROW_NUMBER() over(order by cst_id) customer_key,  
		ci.cst_id as customer_id,  
		ci.cst_key as customer_number,  
		ci.cst_firstname as customer_firstname,  
		ci.cst_lastname as customer_lastname,  
		ci.cst_material_status as customer_material_status,  
		case when cst_gndr is not null then ci.cst_gndr   
		else coalesce(ca.GEN,'n/a') end customer_gender,  
		ca.BDATE as customer_birthdate,  
		la.CNTRY as customer_country  
from Silver.crm_cust_info ci  
left join silver.erp_CUST_AZ12 ca  
on ci.cst_key=ca.CID  
left join silver.erp_LOC_A101 la  
on ci.cst_key=la.CID
""")
conn.commit()
cursor.execute("""
create view Gold.dm_products as
SELECT 
		ROW_NUMBER() over(order by pn.prd_start_dt,pn.prd_id) as peoduct_key,     
		pn.prd_id AS product_id,     
		pn.prd_key AS product_number,     
		pn.prd_nm AS product_name,    
		pn.cat_id AS category_id,     
		pc.cat AS category,     pc.SUBCAT AS subcategory,     
		pc.maintenance,     pn.prd_cost AS cost,     
		pn.prd_line AS product_line,     
		pn.prd_start_dt AS start_date 
FROM silver.crm_prd_info pn LEFT JOIN silver.erp_px_cat_g1v2 pc     
ON pn.cat_id = pc.id WHERE prd_end_dt IS NULL -- Filter out all historical data
""")
conn.commit()
cursor.execute("""
select   
		sd.sls_ord_num as order_number,  
		dp.product_id,  
		dc.customer_key,  
		sd.sls_order_dt as order_date,  
		sd.sls_ship_dt as shipping_date,  
		sd.sls_due_dt as due_date,  
		sd.sls_sales as sales_amount,  
		sd.sls_quantity as quantity,  
		sd.sls_price as price  
from silver.crm_sales_details sd  
left join gold.dm_customers  dc  
on sd.sls_cust_id=dc.customer_id  
left join gold.dm_products dp  
on sd.sls_prd_key=dp.product_number
""")

# --- Close ---
cursor.close()
conn.close()
print('Executed')