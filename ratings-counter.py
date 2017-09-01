# SparkContext is what creates RDDs or Resilient Distibuted Datasets
#SparkConf- configure the SCis where do you want to run, local, cluster?
from pyspark import SparkConf, SparkContext
import collections

#set master node to local machine, not cluster. Running on one processor (local). Set the app name
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#go to local file system and grab the data file
lines = sc.textFile("u.data")
#splitting individual data columns and then grabbing the 3 item in the row (rating of the movie)
ratings = lines.map(lambda x: x.split()[2])
#gets the histogram (3,2),(1,2),(2,1)
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
