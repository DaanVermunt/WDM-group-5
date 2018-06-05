package org.apache.flink.quickstart

import java.time.LocalDate
import java.util.Properties

import com.github.tototoshi.csv._
import net.liftweb.json._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.immutable.HashMap


object ListingsTypeStream {

  def main(args: Array[String]) {

    val pathListings = "/home/aclangerak/Documents/Master_ES/webdata/WDM-group-5/data/listings.csv"

    // Parse the lookup table to a Hashmap for effective lookups from memory

    val reader = CSVReader.open(pathListings)
    var listings : HashMap[Int, Listing] = HashMap.empty
    // skip header
    reader.readNext()
    listings += 0 -> Listing(0, "No city")
    reader.foreach(line => {
      try {
        listings += line.head.toInt -> Listing(line.head.toInt, line(21))
      } catch {
        case e : Exception => println(e)
        // just skip this line
      }
    })

    // kafka properties
    val properties = new Properties()
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink")
    // always read the Kafka topic from the start
    properties.setProperty("auto.offset.reset", "earliest")


    val conf = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env = StreamExecutionEnvironment.createLocalEnvironment(1,conf)

    val stream = env.addSource(new FlinkKafkaConsumer010("Calendar", new SimpleStringSchema(), properties))

    val parsedStream = stream.map(
      record => {
        parse(record)
      }
    )

    val structuredStream:DataStream[CalendarObj] = parsedStream.map(
      record => {
        CalendarObj(
          // ( input \ path to element \\ unboxing ) (extract no x element from list)
          ( record \ "listing_id" \\ classOf[JInt] )(0).toInt
          , ( record \ "date" \\ classOf[JString] )(0).toString
          , ( record \ "available" \\ classOf[JString] )(0).toString
          , ( record \ "price" \\ classOf[JString] ).headOption
        )
      }
    )


    val listingsTypes = structuredStream
      .filter(obj => listings.contains(obj.listing_id))
      .filter(c => c.available == "t")
      .map (c => getCalItemTypeCount(listings, c))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)


    listingsTypes.print()





    env.execute()
  }




  def getCalItemTypeCount(listings: HashMap[Int, Listing], c: CalendarObj): CalItemTypeCount = {
    try {
      CalItemTypeCount(CalItemType(dateToMonth(c.date), listings(c.listing_id).listType), 1)
    } catch {
      case e: Exception => CalItemTypeCount(CalItemType("0000-00", "no type"), 0)
    }
  }


  def dateToMonth(date: String): String = {
    date.dropRight(3)
  }

  case class CalItem(listingId: Int, date: String, available: String)
  case class Listing(listingId: Int, listType: String)
  case class CalItemType(month: String, listType: String)
  case class CalItemTypeCount(calItem: CalItemType, count: Int)

  case class CalendarObj(
                          listing_id:Int
                          , date:String
                          , available:String
                          , price:Option[String]
                        )

}
