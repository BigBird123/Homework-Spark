from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("total_spent")
sc = SparkContext(conf=conf)
def parseLine(RDD):
    field = RDD.split(",") # giờ nó đang là list
    # lưu ý, ta phải truyển đúng dạng các trường data. Không là sẽ sai hoặc xảy ra lỗi.
    # vd: float ko được chuyển thành int, vì số đó có dâu "." sẽ được coi như là một string
    return (int(field[0]),float(field[2]))
data = sc.textFile("file:///SparkCourse/customer-orders.csv")
data_spent = data.map(parseLine)
totalByCustomer = data_spent.reduceByKey(lambda x,y: x+y)
show = totalByCustomer.collect() # nó sẽ trở thành list
print(show[:3])