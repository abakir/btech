import pandas as pd
import urllib2
import json
from pyspark.sql.types import StringType
'''
from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext("local", "generate_new")
sqlContext = SQLContext(sc)
'''
full_data = []
 
# get all products from API
i=1
while True:
    url = 'https://btech.eg/rest/en/V1/products?searchCriteria[currentPage]='+str(i)+'&searchCriteria[pageSize]=500'
    response = urllib2.urlopen(urllib2.Request(url, headers={'Authorization': 'Bearer xkkgxj33ib0uqs9cdcumo9an2g7th4gp'}))
    a = response.read()
    total = json.loads(a)['total_count']
    full_data = full_data + json.loads(a)['items']
    if (i*500 > total):
        break
    i = i + 1
    print i


# normalize JSON files
pd_df = pd.DataFrame()

for i in range(len(full_data)):
    print i
    for key in full_data[i].keys():
        if not isinstance(full_data[i][key], list):
            pd_df.loc[i, key] = full_data[i][key]
        else:
            if len(full_data[i][key]) != 0 and key == "custom_attributes":
                for j in range(len(full_data[i][key])):
                    pd_df.loc[i, full_data[i][key][j]["attribute_code"]] = full_data[i][key][j]["value"]





pd_df = pd_df.drop('description', 1)
pd_df = pd_df.drop('meta_description', 1)
pd_df = pd_df.drop('meta_keyword', 1)

pd_df.to_csv("/dev/shm/prods_temp.csv",index=False,encoding='utf-8')



import datetime
import time

# Orders form Magento

ords = pd.read_csv('/dev/shm/magento_ords.csv')

t1 = (datetime.datetime.strptime(ords.loc[len(ords)-1, 'CreatedAt'], '%Y-%m-%d %H:%M:%S')+ datetime.timedelta(days=1)).strftime('%Y-%m-%d')

t2 = (time.strftime("%Y-%m-%d"))

n = (datetime.datetime.strptime(t2, '%Y-%m-%d') - datetime.datetime.strptime(t1, '%Y-%m-%d')).days


then = t1
for i in range(n):
    print t1
    full_data = []
    url = "https://btech.eg/rest/V1/orders?searchCriteria[filter_groups][0][filters][0][field]=created_at&searchCriteria[filter_groups][0][filters][0][value]="+t1+"%2000:00:00&searchCriteria[filter_groups][0][filters][0][condition_type]=from&searchCriteria[filter_groups][1][filters][1][field]=created_at&searchCriteria[filter_groups][1][filters][1][value]="+then+"%2023:59:59&searchCriteria[filter_groups][1][filters][1][condition_type]=to"
    response = urllib2.urlopen(urllib2.Request(url, headers={'Authorization': 'Bearer xkkgxj33ib0uqs9cdcumo9an2g7th4gp'}))
    a = response.read()
    full_data = full_data + json.loads(a)['items']
    ords = pd.DataFrame(columns=["OrderID", "CustomerID", "SKU", "CreatedAt"])
    for i in range(len(full_data)):
        if 'customer_id' in full_data[i].keys():
            for j in range(len(full_data[i]['items'])):
                ords.loc[len(ords)] = [full_data[i]['items'][j]['order_id'], full_data[i]['customer_id'], full_data[i]['items'][j]['sku'].encode("ASCII", 'ignore'), full_data[i]['created_at']]
    ords.to_csv('/dev/shm/magento_ords.csv',mode='a', index=False, header = False)
    then = (datetime.datetime.strptime(t1, '%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    t1 = then


cust_reqd = pd.read_csv("/dev/shm/magento_ords.csv", index_col=False)

cust_reqd = cust_reqd.drop_duplicates()


sku_encoder = list(set(cust_reqd['SKU'].tolist()))
# generate integers for each sku (algorithm's requirement)
prod_num = pd.DataFrame({'SKU': list(set(sku_encoder)), 'Value': range(len(set(sku_encoder)))})


prod_num.to_csv("/dev/shm/prod_num.csv", index=False)



new_ords = cust_reqd.set_index('SKU').join(prod_num.set_index('SKU'))[['CustomerID', 'Value']]
# create customer * sku(identifier) binary file
ords_bin = pd.crosstab(new_ords.CustomerID, new_ords.Value).applymap(lambda x: 0 if x==0 else 1)

del ords_bin.index.name
del ords_bin.columns.name

ords_bin.to_csv("/dev/shm/cust_prod_bin.csv")

cust_prod_bin = pd.read_csv("/dev/shm/cust_prod_bin.csv", index_col=False)

# convert integers to string for compatibility
sku_encoder1 = prod_num['Value'].tolist()
for i in sku_encoder1:
    sku_encoder1.append(str(i))
    sku_encoder1.remove(i)


# add the skus to cusatomers file with 0s indicating not purchased
sku_cust = cust_prod_bin.columns.tolist()
for i in sku_encoder1:
    if i not in sku_cust:
        cust_prod_bin[i] = 0

#set the customerID as index, retain only skus from feature matrix and transpose
cust_prod_bin = cust_prod_bin.set_index('Unnamed: 0')

cust_prod_bin = cust_prod_bin[sku_encoder1]

cust_prod_bin = cust_prod_bin.set_index('custID')
cust_prod_bin['total_ord'] = cust_prod_bin.sum(axis=1)

cust_prod_bin1 = cust_prod_bin.loc[(cust_prod_bin['total_ord'] > 0)]
cust_prod_new = cust_prod_bin1[sku_encoder1]

# binary file with each customer mapped with every product
new = cust_prod_new.stack()

new.to_csv("/dev/shm/new.csv")

new1 = pd.read_csv("/dev/shm/new.csv", header=None)
new1.to_csv("/dev/shm/new.csv", index=False, header=None)




# Write the file to S3
import boto
import boto.s3
import os
from boto.s3.key import Key

AWS_ACCESS_KEY_ID = 'AKIAJ3QQJCR5ACLVKQRA'
AWS_SECRET_ACCESS_KEY = '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'


conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

bucket = conn.get_bucket('bteckenodes')
path = 'collab_filter' 
key_name = 'new.csv'


file_path = '/dev/shm/'+ key_name

full_key_name = os.path.join(path, key_name)
k = bucket.new_key(full_key_name)
k.set_contents_from_filename(file_path)
k.make_public()

bucket = conn.get_bucket('bteckenodes')
key_name = 'cust_ord_reqd.csv'
path = 'collab_filter' 


file_path = '/dev/shm/'+ key_name

full_key_name = os.path.join(path, key_name)
k = bucket.new_key(full_key_name)
k.set_contents_from_filename(file_path)
k.make_public()


bucket = conn.get_bucket('bteckenodes')
key_name = 'prod_num.csv'
path = 'collab_filter' 


file_path = '/dev/shm/'+ key_name

full_key_name = os.path.join(path, key_name)
k = bucket.new_key(full_key_name)
k.set_contents_from_filename(file_path)
k.make_public()







