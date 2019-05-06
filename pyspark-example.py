from pyspark import SparkContext
sc = SparkContext()
pythonList = [2.3, 3.4, 4.3, 2.4, 2.3, 4.0]

parPythonData = sc.parallelize(pythonList,2)
print(parPythonData.collect())

print(parPythonData.first())

print(parPythonData.take(2))

print(parPythonData.getNumPartitions())

tempData = [59, 57.2, 53.6, 55.4, 51.8, 53.6, 55.4]

def fahrenheitToCentigrade(temperature):
  centigrade = (temperature-32)*5/9

parTempData = sc.parallelize(tempData, 2)

# RDD - Resilient Distributed DAtasets
print(parTempData)

print(parTempData.collect())

rdd = sc.textFile('text_file.txt')
words = rdd.flatMap(lambda x: x.split(' '))
result = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
print(result.collect())
print(result)

print(words.collect())
print(words.map(lambda x: (x,1)).collect())
