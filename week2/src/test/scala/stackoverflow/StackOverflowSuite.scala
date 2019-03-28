package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import stackoverflow.StackOverflow.{rawPostings, sc}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  def initializestack(): Boolean =
    try {
      StackOverflow
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120


    val lines: RDD[String] = sc.parallelize(
      List("1,10,,,2,C++",
        "2,11,,10,4,",
        "2,12,,10,1,",
        "1,13,,,3,PHP",
        "2,14,,13,3,",
        "1,15,,,0,Python"))

    val raw: RDD[Posting] = rawPostings(lines)
    val grouPosting: RDD[(QID, Iterable[(Question, Answer)])]  = groupedPostings(raw)
    val scored: RDD[(Posting, HighScore)]                      = scoredPostings(grouPosting)
    val vectors: RDD[(LangIndex, HighScore)]                   = vectorPostings(scored)


  }


  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("test numéro 1 - Parsing") {
    assert(initializestack(), "revoir le Parsing ")

    val raw = testObject.raw
    val res1 = raw.count() == 6
    val res2 = raw.first().id == 10

    assert(res1)
    assert(res2)

  }

  test("test numéro 1 - filtre and join") {
    assert(initializestack(), "revoir le filtre ou le join ? ")

    val raw = testObject.grouPosting
    val res1 = raw.count() == 2
    val res2 = raw.first()._1 == 13
    val res3 = raw.sortBy({ case (x, y) => x}).first()._1 == 10

    assert(res1)
    assert(res2)
    assert(res3)

  }
  test("test numéro 3 - group posting") {
    assert(initializestack(), "revoir la val end ")

    val raw = testObject.scored
    val res1 = raw.count() == 2
    val res3 = raw.sortBy({ case (x, y) => y}, ascending = false).first()._2 == 4

    assert(res1)
    assert(res3)

  }
  test("test numéro 4 - vector posting") {
    assert(initializestack(), "revoir la fonction ")

    val vectors = testObject.vectors
    val res1 = vectors.count() == 2
    val res2 = vectors.sortBy((tup: (Int, Int)) => tup._1).first() == (100000, 3)
    assert(res1)
    assert(res2)

  }

  test("'kmeans' work.") {
    val a = Array.range(0, 45)
    val b = a.map(_ * 50000)
    val means = b zip a
    val vectors = testObject.vectors
    val newmeans = testObject.kmeans(means, vectors)
    val res1 = newmeans.length == 45
    val res2 = newmeans(2) == (100000, 3) && newmeans(5) == (250000, 4)
    assert(res1)
    assert(res2)
  }

  test("'clusterResults' work.") {
    val a = Array.range(0, 45)
    val b = a.map(_ * 50000)
    val means = b zip a
    means(2) = (100000, 3)
    means(5) = (250000, 4)
    val vectors = testObject.vectors
    val result = testObject.clusterResults(means, vectors)
    val res1 = result(0) == ("PHP", 100.0, 1, 3)
    val res2 = result(1) == ("C++", 100.0, 1, 4)
    assert(res1)
    assert(res2)
  }
}
