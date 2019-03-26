package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.immutable

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Wikipedia")
  val sc: SparkContext = new SparkContext(conf)

  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = {
    sc.textFile(WikipediaData.filePath).map(data => WikipediaData.parse(data)).cache()
  }

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    // accOp = ce qui est calculé en // car les calculs sont split
    // cobOp = resultats des calculs en //
    val accOp = (nb: Int, string :WikipediaArticle) => nb + 1
    val comOp = (nb: Int, nb_2: Int) => nb + nb_2
    rdd.filter(rdd => rdd.mentionsLanguage(lang)).aggregate(zeroValue = (0))(accOp, comOp)

  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    // autre moyen
    // val convert = langs.map(lang => (lang, occurrencesOfLang(lang, rdd)))
    def convert(langs: List[String], rdd : RDD[WikipediaArticle]): List[(String, Int)] = {
      langs match {
        case head :: tail => (head, occurrencesOfLang(head, rdd)) :: convert(tail, rdd)
        case Nil => Nil
        }
      }
    convert(langs, rdd).sortBy(_._1).reverse
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   * l'index inversé sert à stocker à l'avance les resulats pour que la recherche soit plus rapide
   */

  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
      rdd.flatMap(arti => for (x <- langs if arti.mentionsLanguage(x)) yield(x, arti)).groupByKey().cache()
      // or
      // article => langs.collect { case lang if (article.text.split("\\s+").contains(lang)) => (lang, article) })
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   *
   *   Le but et de partir de la liste d'index inverse pour pourvoir la transformer plus facilement et plus
   *   rapidemment. Ceci, dans le but d'avoir une nouvelle liste d'index inverse
   *
   *   mapValue opere sur la value dans un pairRDDs. Tandis que map opere sur tout.
   *   - val result: RDD[(A, C)] = rdd.map { case (k, v) => (k, f(v)) }
   *   - val result: RDD[(A, C)] = rdd.mapValues(f)
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    //val x = List(index.mapValues(b => b.size).sortBy(b => b._2).collect())
    index.mapValues(b => b.size).sortBy(b => b._2).collect().toList
    //println(Console.BLUE+ x)
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = ???

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
