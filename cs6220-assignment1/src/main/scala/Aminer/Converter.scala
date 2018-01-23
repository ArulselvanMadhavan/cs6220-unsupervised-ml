package Aminer
import io.circe.generic.auto._
import io.circe.syntax._
import java.io._

import scala.io.Source

/**
  * Created by amadhavan1 on 1/22/18.
  */
final case class Publication(id: Option[Long],
                             title: Option[String],
                             authors: Array[String],
                             year: Option[Int],
                             venue: Option[String],
                             citations: List[Long])

object Converter {
  final val BUFFER_SIZE = 100
  def toLong(s: String): Option[Long] = {
    try {
      Some(s.toLong)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def parseIndexId(index: String): Option[Long] = {
    if (index.startsWith("#index")) {
      val id: String = index.drop(7)
      toLong(id)
    } else None
  }

  def parseTitle(title: String): Option[String] = {
    if (title.startsWith("#*")) {
      Some(title.drop(3))
    } else None
  }

  def parseAuthors(authorsLine: String): Array[String] = {
    if (authorsLine.startsWith("#@")) {
      authorsLine.drop(3).split(";").filter(_.isEmpty == false)
    } else Array.empty
  }

  def parseYear(yearLine: String): Option[Int] = {
    if (yearLine.startsWith("#t")) {
      toInt(yearLine.drop(3))
    } else None
  }

  def parseVenue(venueLine: String): Option[String] = {
    if (venueLine.startsWith("#c")) {
      Some(venueLine.drop(3))
    } else None
  }

  def parseCitations(citations: Iterator[String]): List[Long] = {
    val result = for {
      citation <- citations
      if citation.startsWith("#%")
    } yield toLong(citation.drop(3))
    result.flatten.toList
  }

  def toPublication(entry: Iterator[String]): Publication = {
    val indexId: Option[Long]  = parseIndexId(entry.next())
    val title: Option[String]  = parseTitle(entry.next())
    val authors: Array[String] = parseAuthors(entry.next())
    val year: Option[Int]      = parseYear(entry.next())
    val venue: Option[String]  = parseVenue(entry.next())
    val citations: List[Long]  = parseCitations(entry)
    Publication(indexId, title, authors, year, venue, citations)
  }

  def writeToFile(writer: BufferedWriter, publications: Array[Publication]): Unit = {
    val text =
      publications.filter(_ != null).map(_.asJson.toString).mkString(",")
    writer.write(text)
  }

  def convertTextToJson(source: String, destination: String): Unit = {
    val inputStream: InputStream = getClass.getResourceAsStream(source)
    val outputWriter: BufferedWriter = new BufferedWriter(
      new OutputStreamWriter(new FileOutputStream("src/main/resources/AP_train.json"), "UTF-8"))
    val inputIterator: Iterator[String] = Source.fromInputStream(inputStream).getLines
    val buffer: Array[Publication]      = new Array[Publication](BUFFER_SIZE)
    var i                               = 0
    outputWriter.write("[\n")
    while (inputIterator.hasNext) {
      val oneEntry: Iterator[String] = inputIterator.takeWhile(_.isEmpty == false)
      buffer(i) = toPublication(oneEntry)
      i += 1
      if (i == BUFFER_SIZE) {
        writeToFile(outputWriter, buffer)
        i = 0
      }
    }
    writeToFile(outputWriter, buffer.take(i))
    outputWriter.write("\n]")
    outputWriter.close()
    inputStream.close()
  }

  def main(args: Array[String]): Unit = {
    convertTextToJson("/AP_train.txt", "/AP_train.json")
  }
}
