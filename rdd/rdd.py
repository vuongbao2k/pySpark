from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Learn RDD").setMaster("local[2]")
sc = SparkContext(conf=conf)

# Init RDD = parallelize
print('=====1=====')
data_list = [1,2,3,4,5]
data = sc.parallelize(data_list)
data.foreach(lambda x: print(x))

# Init RDD = External Dataset
print('=====2=====')
file = sc.textFile('data.txt')
file.foreach(lambda x: print(x))
print('=====3=====')
folder1 = sc.textFile('../folder/*.txt')
folder1.foreach(lambda x: print(x))
print('=====4=====')
folder2 = sc.textFile('../folder/*.zip')
folder2.foreach(lambda x: print(x))
print('=====5=====')
folder3 = sc.textFile('../folder/')
folder3.foreach(lambda x: print(x))
print('=====6=====')
folder4 = sc.wholeTextFiles('../folder/')
folder4.foreach(lambda x: print(x))
print('=====7=====')
file = sc.textFile('data.txt')
file.foreach(lambda x: print(x))
file1 = file.map(lambda x: len(x))
file1.foreach(lambda x: print(x))
file2 = file1.reduce(lambda a,b: a+b)
print(file2)
print('=====8=====')
file3 = file.flatMap(lambda x: x.split(' '))
file3.foreach(lambda x: print(x))
print('=====9=====')
file4 = file3.reduce(lambda a,b: a+b)
print(file4)

