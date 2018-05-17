"""SimpleSparkApp.py"""
#Links to refer 
#Passing list to spark - http://stackoverflow.com/questions/37621853/how-to-pass-list-of-rdds-to-groupwith-in-pyspark
#Installing and setting up Spark - https://www.tutorialspoint.com/apache_spark/apache_spark_quick_guide.htm
#Other useful Spark documentation - https://spark.apache.org/docs/0.9.1/quick-start.html#a-standalone-app-in-python

from pyspark import SparkContext

logFile = "/usr/local/lib/spark-2.0.1-bin-hadoop2.7/README.md"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)

