#Cải thiện word-count  bằng biểu thức chính quy
import re
from pyspark import SparkConf, SparkContext
#
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    #điều này chỉ giải quyết một số vấn đề về mã hóa,
    # vì vậy trong trường hợp một thứ gì đó được mã hóa dưới dạng UTF 8
    # hoặc Unicode trong văn bản gốc của chúng tôi, điều này đảm bảo rằng chúng tôi có thể hiển thị nó.
    # Và thiết bị đầu cuối của chúng tôi trong dấu nhắc lệnh () của chúng tôi bằng cách chuyển đổi nó sang định dạng ASCII,
    # và 'ignore' nó cũng cho  biết chúng tôi sẽ bỏ qua bất kỳ lỗi chuyển đổi nào có thể xảy ra trong quá trình cố gắng chuyển đổi
    # từ Unicode  sang ASCII.
    #############################################################################
    #Vì vậy, đây chỉ là một cách để đảm bảo rằng chúng tôi có thể hiển thị những từ này mà không có lỗi
    # mặc dù chúng có thể chứa các ký tự đặc biệt chỉ là một thủ thuật Python nhỏ cho bạn ở đó.
    #############################################################################

    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
