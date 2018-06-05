package org.apache.flink.quickstart

import java.util.Properties

import net.liftweb.json._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010


object WordCountStream {

  def main(args: Array[String]) {

    // kafka properties
    val properties = new Properties()
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink")
    // always read the Kafka topic from the start
    properties.setProperty("auto.offset.reset", "earliest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new FlinkKafkaConsumer010("Review", new SimpleStringSchema(), properties))

    val parsedStream = stream.map(
      record => {
        parse(record)
      }
    )

    val stopWords = Array("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now")

    val structuredStream: DataStream[ReviewObj] = parsedStream.map(
      record => {
        ReviewObj(
          // ( input \ path to element \\ unboxing ) (extract no x element from list)
          (record \ "listing_id" \\ classOf[JInt]) (0).toInt
          , (record \ "comments" \\ classOf[JString]).headOption
        )
      }
    ).filter(obj => obj.review match {
      case Some(i) => true
      case None => false
    }
    )

    val counts = structuredStream
      .flatMap(l => {
        l.review.get
          .split("\\s")
          .map(s => s.replaceAll("\"", "")
            .replaceAll("\\s", "")
            .toLowerCase())
          .filter(s => s != "")
          .map(s => ListingWord(l.listing_id, s))
      })
      .filter(s => !stopWords.contains(s.word))
      .map { w => ListingWordCounts(w.listing_id, Array(WordCount(w.word, 1))) }
      .keyBy("listing_id")
      .timeWindow(Time.seconds(100))
      .reduce((el1, el2) => wordCountPerListingReducer(el1, el2))
      .map(w => getHighestFive(w))

    counts.print()

    env.execute()
  }


  case class WordCount(word: String, count: Long)
  case class ListingWord(listing_id: Int, word: String)
  case class ListingWordCounts(listing_id: Int, wordCounts: Array[WordCount]) {

    override def toString: String = {
      var res = "{ listing_id : "
      res += listing_id
      res += " , [ "
      wordCounts.foreach(wc => {
        res += wc.word
        res += " : "
        res += wc.count
        res += ", "
      })
      res += "] }"
      res.toString()
    }
  }

  case class ReviewObj(
                        listing_id: Int
                        , review: Option[String]
                      )


  def wordCountPerListingReducer(el1: ListingWordCounts, el2: ListingWordCounts): ListingWordCounts = {
    var wordCounts = el1.wordCounts

    el2.wordCounts.foreach(wc => {
      if(!wordCounts.map(wc1 => wc1.word).contains(wc.word)) {
        wordCounts = wordCounts :+ wc
      } else {
        wordCounts = wordCounts.map(wc1 => {
          if (wc1.word == wc.word) {
            WordCount(wc1.word, wc1.count + wc.count)
          } else {
            wc1
          }
        })
      }
    })

    ListingWordCounts(el1.listing_id, wordCounts.distinct)
  }

  def getHighestFive(counts: ListingWordCounts): ListingWordCounts = {
    ListingWordCounts(counts.listing_id, counts.wordCounts.sortWith(_.count > _.count).take(5))
  }


}
