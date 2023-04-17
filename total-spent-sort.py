from pyspark import  SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomerAndSort")
sc = SparkContext(conf=conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:///sparkcourse/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)
# đổi key/vlue -> value/key rồi ta xắp xếp theo giá trị mới
flipped = totalByCustomer.map(lambda i: (i[1],i[0]))
totalCustomerSorted =flipped.sortByKey().collect()
for value in totalCustomerSorted:
    print("Custome ID {} spent {} amount".format(value[1], value[0]))
