package flinkScala

import org.apache.flink.api.scala._

object ListingTypeBatch {

  def main(args: Array[String]): Unit = {
    val pathCal = "file:///home/daan/Documents/TuD/WDM/project/WDM-group-5/data/" + args(0)
    val pathListings = "file:///home/daan/Documents/TuD/WDM/project/WDM-group-5/data/" + args(1)
    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // load review
    val cal = env.readCsvFile[CalItem](pathCal, lineDelimiter = "\n", fieldDelimiter = ",", ignoreFirstLine = false, lenient = true, quoteCharacter = '"')
    val listings = env.readCsvFile[Listing](pathListings, lineDelimiter = "\n", fieldDelimiter = ",", ignoreFirstLine = true, lenient = true, includedFields = Array(0, 27), quoteCharacter = '"')

    val calType = cal
      .filter(c => c.available == "t")
      .join(listings).where(0).equalTo(0) { (r, l) => CalItemTypeCount(CalItemType(dateToMonth(r.date), l.listType), 1)}
      .groupBy(0)
      .sum(1)

    calType.print()
  }

  def dateToMonth(date: String): String = {
    date.dropRight(3)
  }

  case class CalItem(listingId: Int, date: String, available: String)
  case class Listing(listingId: Int, listType: String)
  case class CalItemType(month: String, listType: String)
  case class CalItemTypeCount(calItem: CalItemType, count: Int)

}