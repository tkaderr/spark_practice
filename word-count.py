from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("book.txt")
#breaks it to individual words and breakline each word
words = input.flatMap(lambda x: x.split())
#returns dictionary
wordCounts = words.countByValue()
#dictionary into a tuple
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
