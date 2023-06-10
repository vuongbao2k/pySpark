import os
import sys

import pyspark

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Thuc hanh RDD").setMaster("local[2]")
sc = SparkContext(conf=conf)

rdd = sc.textFile('data.txt')
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))
#rdd3.cache()
#rdd3.persist()
rdd3.persist(pyspark.StorageLevel.MEMORY_ONLY)
#rdd3.unpersist()
rdd4 = rdd3.reduceByKey(lambda a,b: a+b)
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey(False)
rdd6 = rdd5.filter(lambda x: 'an' in x[1])
rdd7 = rdd6.groupByKey().mapValues(list)
rdd8 = rdd3.distinct()
print(rdd6.collect())
print(rdd7.collect())
#print(rdd8.collect())

r1 = sc.parallelize([(1,11),(2,22),(3,33),(4,44),(5,55)])
r2 = sc.parallelize([(3,'a'),(4,44),(5,'c'),(6,'d'),(7,'e')])
r3 = r1.union(r2)
r4 = r1.intersection(r2)
r5 = r1.join(r2)
#print(r5.collect())

# Action - count
print("Count : "+str(rdd6.count()))
# Action - first
firstRec = rdd6.first()
print("First Record : "+str(firstRec[0]) + ","+ firstRec[1])
# Action - max
datMax = rdd6.max()
print("Max Record : "+str(datMax[0]) + ","+ datMax[1])
# Action - reduce
totalWordCount = rdd6.reduce(lambda a,b: (a[0]+b[0],a[1]))
print("dataReduce Record : "+str(totalWordCount[0]))
# Action - take
data3 = rdd6.take(3)
for f in data3:
    print("data3 Key:"+ str(f[0]) +", Value:"+f[1])
# Action - collect
data = rdd6.collect()
for f in data:
    print("Key:"+ str(f[0]) +", Value:"+f[1])
# Action - saveAsTextFile
rdd6.saveAsTextFile("wordCount")

#tong = sc.accumulator()

