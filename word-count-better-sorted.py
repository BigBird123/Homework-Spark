import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)
#chúng ta có thể sắp xếp những gì CountByValue() trả về, nhưng hãy sử dụng RDD để giữ cho nó có thể mở rộng ( scalable).
# Ta có thể sử dụng CountByValue(), những hãy làm chút gì đó khó hơn 1 chút.
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
# thấy không thay vì gọi CountByValue(), về cơ bản ta đã thực hiện bằng tay.

wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results =  wordCountsSorted.collect()
print(results)
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
