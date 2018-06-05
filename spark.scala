//Most of the implementation are done on Spark-SQL to better understand Spark's performance with MySQL/ Cassandra

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

//2. remove stop words from the reviews
//val stopWords = Set("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "Now", "I", "His", "Her", "My", "Their", "Our")

//val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("text")
//val countTokens = udf{(words: Seq[String]) => words.length}
//val tokenized = tokenizer.transform(reviews)
//tokenized.select("comments","text").withColumn("tokens", countTokens(col("text"))).show()

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
spark.time((session.sql("select listing_id, sum(case when available='f' then 1 END) from calendarView group by listing_id order by sum(case when available='f' then 1 END) desc" )).show())
