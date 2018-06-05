// Most of the implementation are done on Spark-SQL to better understand Spark's performance with MySQL/ Cassandra.
// The data loaded to Spark are of the subsets of calendar(1.4 GB), listing(368.3) and reviews(1.2 GB) 
// The large data sizes lead to high load time, but is justified to evaluate the performance of Spark on large data dumps
// Spark-SQL uses the Spark-Core and is substantially faster than the previous RDD methods (which is fexible, but slow)

//imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions._

val session = SparkSession.builder().appName("test").getOrCreate()
val sqlContext= new org.apache.spark.sql.SQLContext(sc)

//loading data as dataframe
val calendar = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/home/spark/Downloads/WDM-group-5/data/calendar.csv")
val reviews = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/home/spark/Downloads/WDM-group-5/data/reviews.csv")
val listing = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load("/home/spark/Downloads/WDM-group-5/data/listings.csv")

//validating loaded data
calendar.printSchema()
reviews.printSchema()
listing.printSchema()


//data cleaning #1 - remove rows where id is not numeric
val tempListing = listing.select("*").filter($"id" rlike "^([0-9])+$")
val cleanReviews = reviews.select("*").filter($"listing_id" rlike "^([0-9])+$")
val cleanCalendar = calendar.select("*").filter($"listing_id" rlike "^([0-9])+$")

//data cleaning #2 - remove cities that are not cities based on longest city name = 17 letters
val cleanListing = tempListing.select("*").filter((len(col("name")) < 16))

//data cleaning #3 - remove stopwords from reviews
// 1. create a new attribute of unigrams = "words"
val countReviews = cleanReviews.select($"comments", explode(split($"comments", " ")).as("words"))
val count =  countReviews.groupBy("words").count()

//2. remove stop words from the reviews - Produces Runtime Error
//2.1. Tokenize the review text as unigrams
//val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("text")
//val countTokens = udf{(words: Seq[String]) => words.length}
//val tokenized = tokenizer.transform(reviews)
//tokenized.select("comments","text").withColumn("tokens", countTokens(col("text"))).show()
//2.2. StopWord Removal
//val remover = new StopWordsRemover()
//remover.setInputCol("text").setOutputCol("filtered")
//remover.transform(tokenized).show()

//register df as temp sql view
cleanListing.createOrReplaceTempView("listingView")
cleanReviews.createOrReplaceTempView("reviewsView")
cleanCalendar.createOrReplaceTempView("calendarView")


// test query = * can be replaced with attributes 
spark.time((session.sql("select * from listingView")).show())

// word count query 
count.createOrReplaceTempView("counts")
spark.time((session.sql("select * from counts order by count desc" )).show())


//total count based on distinct listings that are booked
spark.time((session.sql("select count(distinct listing_id)  from calendarView where available = 'f'" )).show())

//counting unavailable listings and grouping them based on listing_id
spark.time((session.sql("select(select listing_id, sum(case when available='f' then 1 END) from calendarView group by listing_id order by sum(case when available='f' then 1 END) desc)" )).show())

//Scala Data Frame alternative 
//calendar.groupBy("listing_id").count().filter(calendar("available") === "f")

// popular city based on month
//join name,neighborhood,city,available,date,month,listing_id from calendar, listings
val df = calendar.join(listing,listing("id")===calendar("listing_id"))

df.createOrReplaceTempView("combinedData")

//generates an SQL parser Exception
//spark.time((session.sql("select id, name, room_type, property_type, neighborhood_overview, city, sum(case when available='f' then 1 END) from combinedData group by listing_id order by sum(case when available='f' then 1 END) desc)" )).show())








