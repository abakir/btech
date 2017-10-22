import pandas as pd
import numpy as np
import urllib3
import json


import datetime
import time

loc = "/Users/saisreekamineni/Documents/Btech/Recommendation/output_files/"
# Orders form Magento

ords = pd.read_csv(loc + "magento_ords.csv")

t1 = (datetime.datetime.strptime(ords.loc[len(ords)-1, 'CreatedAt'], '%Y-%m-%d %H:%M:%S')+ datetime.timedelta(days=1)).strftime('%Y-%m-%d')

t2 = (time.strftime("%Y-%m-%d"))

n = (datetime.datetime.strptime(t2, '%Y-%m-%d') - datetime.datetime.strptime(t1, '%Y-%m-%d')).days


then = t1
for i in range(n):
    print(t1)
    full_data = []
    http = urllib3.PoolManager()
    r = http.request('GET',"https://btech.eg/rest/V1/orders?searchCriteria[filter_groups][0][filters][0][field]=created_at&searchCriteria[filter_groups][0][filters][0][value]="+t1+"%2000:00:00&searchCriteria[filter_groups][0][filters][0][condition_type]=from&searchCriteria[filter_groups][1][filters][1][field]=created_at&searchCriteria[filter_groups][1][filters][1][value]="+then+"%2023:59:59&searchCriteria[filter_groups][1][filters][1][condition_type]=to")

    full_data = full_data + json.loads(r.data.decode('utf-8'))['items']

    ords = pd.DataFrame(columns=["OrderID", "CustomerID", "SKU", "CreatedAt"])
    for i in range(len(full_data)):
        if 'customer_id' in full_data[i].keys():
            for j in range(len(full_data[i]['items'])):
                ords.loc[len(ords)] = [full_data[i]['items'][j]['order_id'], full_data[i]['customer_id'], full_data[i]['items'][j]['sku'].encode("ASCII", 'ignore'), full_data[i]['created_at']]
    ords.to_csv(loc + "magento_ords.csv",mode='a', index=False, header = False)
    then = (datetime.datetime.strptime(t1, '%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    t1 = then

'''

cat_id_pd = pd.read_csv(loc + "prod_catid.csv",
                                dtype={'category_id': 'str'})
order_pd = pd.read_csv(loc + "magento_ords.csv", 
                      dtype={'CustomerID': 'str'})

sku_2 = list(set(order_pd['SKU']))
sku_1 = list(set(cat_id_pd['sku']))


skus = []
cat_id = []

# include all the SKUs and category ids for the products that were missing in the file
for i in sku_2:
	if i not in sku_1:
		url = 'https://btech.eg/rest/en/V1/products/'+i
		print url
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
new.to_csv(loc + "prod_catid.csv", mode='a', header=False, index=False)

'''
cat_id_pd = pd.read_csv(loc + "prod_catid.csv",
                                dtype={'category_id': 'str'})
order_pd = pd.read_csv(loc + "magento_ords.csv",
                      dtype={'CustomerID': 'str'})

order_pd = order_pd.drop_duplicates()
order_pd = order_pd.drop('OrderID', 1)

quant = pd.DataFrame(order_pd)
order_final = order_pd.sort_values(['CustomerID', 'SKU', 'CreatedAt']).groupby(['CustomerID', 'SKU'], as_index = False).last()
order_final['month'] = pd.DatetimeIndex(order_final['CreatedAt']).month
order_final = order_final.drop('CreatedAt', 1)
order_final = order_final[['CustomerID', 'SKU', 'month']]

quant['Quantity'] = 1
quant_final = quant.groupby(['CustomerID', 'SKU'], as_index = False).sum()

orders_total = quant_final.merge(order_final)


cat_id_pd.set_index('sku', inplace= True)
cat_id_pd['SKU'] = cat_id_pd.index

orders_total.set_index('SKU', inplace= True)

merged = cat_id_pd.join(orders_total, how='inner')
print(len(merged))

final = pd.get_dummies(merged)
#final.to_csv(loc + "fm_input_sample.csv", index=False)
