from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
print(result.items())
sortedResults = collections.OrderedDict(sorted(result.items())) # dict()cũng được, tuy nhiên lưu ý dict và orderDict khác nhau nhé
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
