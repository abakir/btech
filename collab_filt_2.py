'''
wget https://s3-eu-west-1.amazonaws.com/bteckenodes/collab_filter/new.csv
hdfs dfs -put ./new.csv /
rm new.csv 
wget https://s3-eu-west-1.amazonaws.com/bteckenodes/collab_filter/prod_num.csv

pyspark
'''
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

# load data
data = sc.textFile("/new.csv")

# split data into train and test
train, test = data.randomSplit([0.8,0.2])

#parse train and test data
ratings_train = train.map(lambda l: l.split(','))\
    .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

ratings_test = test.map(lambda l: l.split(','))\
    .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

# build model using train data

rank = 100
numIterations = 10

model = ALS.trainImplicit(ratings_train, rank, numIterations, alpha=0.01)


#save model
model.save(sc, "/model1")



from pyspark.mllib.recommendation import MatrixFactorizationModel
import pandas as pd
import json
# load model
sameModel = MatrixFactorizationModel.load(sc, "/model1")

# generate lists of 10 recommended items for all users
all_rec = sameModel.recommendProductsForUsers(10)


a = all_rec.toDF()

df = pd.DataFrame(columns = ['user', 'product', 'rating'])
# convert spark rdd to pandas df
final = pd.concat([all_rec.toDF().select('_2._1.user','_2._1.product', '_2._1.rating').toPandas(),
	all_rec.toDF().select('_2._2.user','_2._2.product', '_2._2.rating').toPandas(),
	all_rec.toDF().select('_2._3.user','_2._3.product', '_2._3.rating').toPandas(),
	all_rec.toDF().select('_2._4.user','_2._4.product', '_2._4.rating').toPandas(),
	all_rec.toDF().select('_2._5.user','_2._5.product', '_2._5.rating').toPandas(),
	all_rec.toDF().select('_2._6.user','_2._6.product', '_2._6.rating').toPandas(),
	all_rec.toDF().select('_2._7.user','_2._7.product', '_2._7.rating').toPandas(),
	all_rec.toDF().select('_2._8.user','_2._8.product', '_2._8.rating').toPandas(),
	all_rec.toDF().select('_2._9.user','_2._9.product', '_2._9.rating').toPandas(),
	all_rec.toDF().select('_2._10.user','_2._10.product', '_2._10.rating').toPandas()])

prod_num = pd.read_csv("/home/hadoop/prod_num.csv")

# match sku with integer number assigned
final = final.set_index('product').join(prod_num.set_index('Value'))[['user', 'SKU', 'rating']]

final = final.sort_values(['user','rating'], ascending = False).reset_index().drop('index', 1)

header = list(set(final['user'].tolist()))
header.sort(reverse = True)
# create JSON
i = 0
rank =1
results = {}
for id in header:
	cat = []
	for rank in range(10):
		prod_sku = {}
		prod_sku['SKU'] = final.loc[i, 'SKU']
		prod_sku['Rating'] = final.loc[i, 'rating']
		prod_sku['Rank'] = rank + 1
		i = i + 1
		cat.append(prod_sku)
	results[id] = cat



with open('/home/hadoop/collab_filter.json', 'w') as outfile:
    json.dump(results, outfile)




# Write the file to S3
import boto
import boto.s3
import os
from boto.s3.key import Key

AWS_ACCESS_KEY_ID = 'AKIAJ3QQJCR5ACLVKQRA'
AWS_SECRET_ACCESS_KEY = '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'
REGION_HOST = 's3-eu-west-1.amazonaws.com'

#conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, host=REGION_HOST)

conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, host=REGION_HOST)

bucket = conn.get_bucket('bteckenodes')
key_name = 'collab_filter.json'
path = 'collab_filter' 

file_path = '/home/hadoop/collab_filter.json'

full_key_name = os.path.join(path, key_name)
k = bucket.new_key(full_key_name)
k.set_contents_from_filename(file_path)
k.make_public()



