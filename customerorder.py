from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrder")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

lines = sc.textFile("customer-orders.csv")
parsedLines = lines.map(parseLine)
# arr = parsedLines.reduceByKey(lambda x,y: x+y).sortByValue
customerSorted = arr.map(lambda x: (x[1], x[0])).sortByKey()
results = customerSorted.collect()
# results = arr.collect()

for result in results:
    print(result)
