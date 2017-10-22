'''
Document: Data preparation script for Category Sorting
Created Date: 20 May 2017
Author: Sai
Purpose : To prepare a file with SKU sorted based on orders over last 30 days
 
--------
'''

import pandas as pd
from pyspark.sql.types import StringType, StructType, StructField, FloatType, IntegerType
import urllib2
import json
import datetime


from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext("local", "cat_sorted_automated_ec2")
sqlContext = SQLContext(sc)

# Columns of new dataframe
columns = ["SKU", "ProductName", "Price", "discount_value", "discount_rate", "no_interest", "disc_int"]

full_data = []

# get all products from Magento API
i=1
while True:
    url = 'https://btech.eg/rest/en/V1/products?searchCriteria[currentPage]='+str(i)+'&searchCriteria[pageSize]=500'
    response = urllib2.urlopen(urllib2.Request(url, headers={'Authorization': 'Bearer xkkgxj33ib0uqs9cdcumo9an2g7th4gp'}))
    a = response.read()
    #print i
    total = json.loads(a)['total_count']
    full_data = full_data + json.loads(a)['items']
    if (i*500 > total):
        break
    i = i + 1

# create empty dataframe with columns
empty = pd.DataFrame(0, index=range(len(full_data)), columns=columns)

# extract all required fields from the products
for i in range(len(full_data)):
	#print i
	empty.loc[i, 'SKU'] = full_data[i]['sku']
	if 'name' in full_data[i].keys():
		empty.loc[i, 'ProductName'] = full_data[i]['name']
	else:
		empty.loc[i, 'ProductName'] = 0
	if 'price' in full_data[i].keys():
		empty.loc[i, 'Price'] = float(full_data[i]['price'])
	else:
		empty.loc[i, 'Price'] = float(0)
	x = empty.loc[i, 'Price']
	for j in range(len(full_data[i]['custom_attributes'])):
		if full_data[i]['custom_attributes'][j]['attribute_code'] == "special_price":
			empty.loc[i, 'discount_value'] = float(x - float(full_data[i]['custom_attributes'][j]['value']))
			empty.loc[i, 'discount_rate'] = float((x - float(full_data[i]['custom_attributes'][j]['value']))*100/x)
			break
		else:
			empty.loc[i, 'discount_rate'] = float(0)
			empty.loc[i, 'discount_value'] = float(0)
	for j in range(len(full_data[i]['custom_attributes'])):
		if full_data[i]['custom_attributes'][j]['attribute_code'] == "installments_no_interest":
			if len(full_data[i]['custom_attributes'][j]['value'])>0:
				empty.loc[i, 'no_interest'] = int(1)
			else:
				empty.loc[i, 'no_interest'] = int(0)
			break
		else:
			empty.loc[i, 'no_interest'] = int(0)
	empty.loc[i, 'disc_int'] = float(empty.loc[i,'discount_rate']*empty.loc[i,'no_interest'])


# change pandas dataframe to spark daatframe
schema= StructType([
	StructField("SKU",StringType(),False),
	StructField("ProductName",StringType(),False),
	StructField("Price",FloatType(),False),
	StructField("discount_value",FloatType(),False),
	StructField("discount_rate",FloatType(),False),
	StructField("no_interest",IntegerType(),False),
	StructField("disc_int",FloatType(),False)])


prods_df = sqlContext.createDataFrame(empty, schema)
##prods_df = spark.createDataFrame(empty, schema)


# for orders


columns = ["OrderID", "SKU", "OrderDate"]

full_data = []

# todays date
end = datetime.datetime.now()
end_d = end.strftime("%Y-%m-%d")

# 30 days before today
start = datetime.datetime.now() + datetime.timedelta(-30)
start_d = start.strftime("%Y-%m-%d")

# get orders over last 30 days using Magento API
i=1
while True:
    url = 'https://btech.eg/rest/V1/orders?searchCriteria[currentPage]='+str(i)+'&searchCriteria[pageSize]=1000&searchCriteria[filter_groups][0][filters][0][field]=created_at&searchCriteria[filter_groups][0][filters][0][value]='+start_d+' 00:00:00&searchCriteria[filter_groups][0][filters][0][condition_type]=from&searchCriteria[filter_groups][1][filters][0][field]=created_at&searchCriteria[filter_groups][1][filters][0][value]='+end_d+' 00:00:00&searchCriteria[filter_groups][1][filters][0][condition_type]=to'
    response = urllib2.urlopen(urllib2.Request(url, headers={'Authorization': 'Bearer xkkgxj33ib0uqs9cdcumo9an2g7th4gp'}))
    a = response.read()
    #print i
    total = json.loads(a)['total_count']
    full_data = full_data + json.loads(a)['items']
    if (i*1000 > total):
        break
    i = i + 1


empty = pd.DataFrame(0, index=range(len(full_data)), columns=columns)

# get required columns from orders data
for i in range(len(full_data)):
	#print i
	empty.loc[i, 'OrderDate'] = full_data[i]['created_at']
	for j in range(len(full_data[i]['items'])):
		empty.loc[i, 'SKU'] = full_data[i]['items'][j]['sku']
		empty.loc[i, 'OrderID'] = full_data[i]['items'][j]['order_id']


# convert orders data frame in pandas to spark df
schema= StructType([
	StructField("OrderID",StringType(),False),
	StructField("SKU",StringType(),False),
	StructField("OrderDate",StringType(),False)])

ords_df = sqlContext.createDataFrame(empty, schema)
##ords_df = spark.createDataFrame(empty, schema)

# confirm the orders are 30 days from today 
ords_df.createOrReplaceTempView("ords_df")
ords_reqd = sqlContext.sql("SELECT OrderID, SKU from ords_df where DATEDIFF(OrderDate, '{}') <= 30".format(end_d))
##ords_reqd = spark.sql("SELECT OrderID, SKU from ords_df where DATEDIFF(OrderDate, '{}') <= 30".format(end_d))

ords_reqd = ords_reqd.dropDuplicates()

ords_reqd.createOrReplaceTempView("ords_reqd")

# Count total orders for each product ID
ord_total = sqlContext.sql("SELECT SKU, count(OrderID) as total_ord from ords_reqd group by SKU order by total_ord desc")
##ord_total = spark.sql("SELECT SKU, count(OrderID) as total_ord from ords_reqd group by SKU order by total_ord desc")

ord_total.createOrReplaceTempView("ord_total")
prods_df.createOrReplaceTempView("prods_df")

##prod_final = spark.sql("SELECT prods_df.SKU as SKU, ProductName, Price, discount_value, discount_rate, no_interest, disc_int, total_ord from \
	##prods_df left outer join ord_total on prods_df.SKU = ord_total.SKU")

# get all the ordered products who have data in products API
prod_final = sqlContext.sql("SELECT prods_df.SKU as SKU, ProductName, Price, discount_value, discount_rate, no_interest, disc_int, total_ord from \
	prods_df left outer join ord_total on prods_df.SKU = ord_total.SKU")

cat = prod_final.fillna(0, subset=['total_ord'])

import numpy as np
cat_np = np.asarray(cat)

# initiate group_rank
group_rank = np.zeros(cat.count())
group_rank = [group_rank,1]

# group ranking function

def g_rank(discount_rate, no_interest, disc_int):
    '''returns sorted groups - first level sorting'''
    if float(disc_int) >= 1:
        group_rank = 1
    elif float(discount_rate) > 10:
        group_rank = 2
    elif int(no_interest) > 0:
        group_rank = 3
    elif 0 < float(discount_rate) < 10:
        group_rank = 4
    else:
        group_rank = 5
    return group_rank


from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

g_rank_Udf = udf(g_rank, IntegerType())

cat.createOrReplaceTempView('cat')

# concatenate group rank col
cat2 = cat.withColumn('group_rank', g_rank_Udf(cat.discount_rate, cat.no_interest, cat.disc_int))

cat2 = cat2.withColumn("total_ord", cat2["total_ord"].cast(IntegerType()))


cat2.createOrReplaceTempView('cat2')

cat2_pd = cat2.toPandas()

# file with category ID for each SKU
cat_id_pd = pd.read_csv("/home/ec2-user/prod_catid.csv")

sku_2 = list(cat2_pd['SKU'])
sku_1 = list(set(cat_id_pd['sku']))

sku_new = []

skus = []
cat_id = []

# include all the SKUs and category ids for the products that were missing in the file
for i in sku_2:
	if i not in sku_1:
		url = 'https://btech.eg/rest/en/V1/products/'+i
		##print url
		try:
			response = urllib2.urlopen(urllib2.Request(url, headers={'Authorization': 'Bearer xkkgxj33ib0uqs9cdcumo9an2g7th4gp'}))
			a = response.read()
			val = json.loads(a)['custom_attributes']
			for item in val:
				if item['attribute_code'] == 'category_ids':
					for id in item['value']:
						skus.append(sku)
						cat_id.append(int(id))
					break
		except:
			continue

new = pd.DataFrame({'sku': skus, 'category_id': cat_id})
new.to_csv("/home/ec2-user/prod_catid.csv", mode='a', header=False, index=False)

cat_id_pd = pd.read_csv("/home/ec2-user/prod_catid.csv")


new_cat = cat_id_pd.merge(cat2_pd, left_on = 'sku', right_on = 'SKU', how = 'left')[['category_id', 'SKU', 'Price', 'discount_value', 'discount_rate', 'no_interest', 'disc_int', 'total_ord', 'group_rank']]

schema= StructType([
	StructField("category_id",IntegerType(),False),
	StructField("SKU",StringType(),False),
	StructField("Price",FloatType(),False),
	StructField("discount_value",FloatType(),False),
	StructField("discount_rate",FloatType(),False),
	StructField("no_interest",FloatType(),False),
	StructField("disc_int",FloatType(),False),
	StructField("total_ord",FloatType(),False),
	StructField("group_rank",FloatType(),False)])


##cat3 = spark.createDataFrame(new_cat, schema)
cat3 = sqlContext.createDataFrame(new_cat, schema)

cat3.createOrReplaceTempView('cat3')

##cat_sorted = spark.sql("SELECT SKU, category_id from cat3 ORDER BY category_id ASC, group_rank ASC, discount_rate DESC, total_ord DESC, discount_value DESC")

cat_sorted = sqlContext.sql("SELECT SKU, category_id from cat3 ORDER BY category_id ASC, group_rank ASC, discount_rate DESC, total_ord DESC, discount_value DESC")
cat_sorted_p = cat_sorted.toPandas()

# Create JSON file
results = {}
ID = cat_sorted_p.loc[0, 'category_id']
j = 1
cat = {}
for i in range(0, len(cat_sorted_p)):
	if ID == cat_sorted_p.loc[i, 'category_id']:
		cat[cat_sorted_p.loc[i, 'SKU']] = j
		j = j + 1
	elif ID != cat_sorted_p.loc[i, 'category_id']:
		results[ID] = cat
		ID = cat_sorted_p.loc[i, 'category_id']
		j = 1
		cat = {}
		cat[cat_sorted_p.loc[i, 'SKU']] = j
		j = j + 1



with open('/home/ec2-user/category_sorting/cat_sorted_reqd.json', 'w') as outfile:
    json.dump(results, outfile)


# Write the file to S3
import boto
import boto.s3
import os
from boto.s3.key import Key

AWS_ACCESS_KEY_ID = 'AKIAJ3QQJCR5ACLVKQRA'
AWS_SECRET_ACCESS_KEY = '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'


conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

bucket = conn.get_bucket('bteckenodes')
key_name = 'cat_sorted_reqd.json'
path = 'category_sorting' 

file_path = '/home/ec2-user/category_sorting/cat_sorted_reqd.json'

full_key_name = os.path.join(path, key_name)
k = bucket.new_key(full_key_name)
k.set_contents_from_filename(file_path)
k.make_public()

