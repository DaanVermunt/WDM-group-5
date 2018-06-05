package flinkScala

import org.apache.flink.api.scala._

object PopularityBatch {

  def main(args: Array[String]): Unit = {
    val pathRevs = "file:///home/daan/Documents/TuD/WDM/project/WDM-group-5/data/" + args(0)
    val pathListings = "file:///home/daan/Documents/TuD/WDM/project/WDM-group-5/data/" + args(1)
    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // load review
    val reviews = env.readCsvFile[Review](pathRevs, lineDelimiter = "\n", fieldDelimiter = ",", ignoreFirstLine = true, lenient = true, quoteCharacter = '"')
    val listings = env.readCsvFile[Listing](pathListings, lineDelimiter = "\n", fieldDelimiter = ",", ignoreFirstLine = true, lenient = true, includedFields = Array(0, 21), quoteCharacter = '"')

    val reviewLocs = reviews
        .join(listings).where(0).equalTo(0) { (r, l) => ReviewLoc(r.date, l.city)}

     val popular = reviewLocs
         .map(r => DateLocCount(dateToMonth(r.date), DateLoc(dateToMonth(r.date), r.loc), 1))
         .groupBy(1)
         .sum(2)
         .groupBy(0)
         .max(2)


    popular.print()
  }

  def dateToMonth(date: String): String = {
    date.dropRight(3)
  }

  case class DateLoc(month: String, loc: String)
  case class DateLocCount(date: String, dl: DateLoc, count: Int)
  case class DateCount(date: String, count: Int)
  case class Review(listing_id: Int, id: Int, date: String, reviewer_id: Int, reviewer_name: String, review: String)
  case class Listing(listingId: Int, city: String)
  case class ReviewLoc(date: String, loc: String)

}