package flinkScala

import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer

object WordCountBatch {

  def main(args: Array[String]): Unit = {
    val path = "file:///home/daan/Documents/TuD/WDM/project/WDM-group-5/data/" + args(0)
    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val stopWords = Array("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now")
    val text = env.readCsvFile[Review](path, lineDelimiter = "\n", fieldDelimiter = ",", ignoreFirstLine = false, lenient = true)

    val counts = text
      .flatMap { r => {
        r.review.split("\\s")
          .map(s => s.replaceAll("\"", "").replaceAll("\\s", "").toLowerCase())
          .filter(s => s != "")
          .map(s => ListingWord(r.listing_id, s))
      } }
      .filter( s => !stopWords.contains(s.word))
      .map { w => ListingWordCounts(w.listing_id , Array(WordCount(w.word, 1))) }
      .groupBy("listing_id")
      .reduce( (el1, el2) => wordCountPerListingReducer(el1, el2))
      .map( w => getHighestFive(w))

    counts.print()
  }

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

  // Data type for words with count
  case class WordCount(word: String, count: Long)
  case class ListingWord(listing_id : Int, word: String)
  case class ListingWordCounts(listing_id: Int, wordCounts: Array[WordCount]){

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


  case class Review(listing_id: Int, id: Int, date: String, reviewer_id: Int, reviewer_name: String, review: String)

}