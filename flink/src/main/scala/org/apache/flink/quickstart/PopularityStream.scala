package org.apache.flink.quickstart

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties

import net.liftweb.json._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.immutable.HashMap
import com.github.tototoshi.csv._
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


object PopularityStream {

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

    val stream = env.addSource(new FlinkKafkaConsumer010("Review", new SimpleStringSchema(), properties))

    val parsedStream = stream.map(
      record => {
        parse(record)
      }
    )

    val structuredStream: DataStream[ReviewObj] = parsedStream.map(
      record => {
        ReviewObj(
          // ( input \ path to element \\ unboxing ) (extract no x element from list)
          (record \ "listing_id" \\ classOf[JInt]) (0).toInt
          , (record \ "date" \\ classOf[JString]).toString
        )
      }
    )


    val popular = structuredStream
      .filter(obj => listings.contains(obj.listing_id))
      .map(r => DateLoc(r.date, listings(r.listing_id).city))
      .map(dl => DateLocCount(dateToMonth(dl.month), DateLoc(dateToMonth(dl.month), dl.loc), 1))
      .keyBy(1)
      .sum(2)
      .keyBy(0)
      .timeWindow(Time.seconds(15))
      .max(2)

    popular.print()

    env.execute()
  }

  def dateToMonth(date: String): String = {
    date.dropRight(3)
  }

  case class DateLoc(month: String, loc: String)
  case class DateLocCount(date: String, dl: DateLoc, count: Int)
  case class DateCount(date: String, count: Int)

  case class Review(listing_id: Int, date: String)
  case class ReviewLoc(date: String, loc: String)

  case class Listing(listingId: Int, city: String)





  case class ReviewObj(
                        listing_id: Int
                        , date: String
                      )
}
