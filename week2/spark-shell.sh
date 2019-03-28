import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

val conf = new SparkConf().setMaster("local").setAppName("xxx")

val sc = new SparkContext(conf)

val text = sc.textFile("/Users/kaiim/Downloads/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv")


text.take(4).foreach(println)

-----------------------
definition des types
-----------------------
object stackOverFlow {
     | type QID = Int
     |   type HighScore = Int
     |   type LangIndex = Int
     | }


----------------------
Création de la class Posting qui va servir de reference 
----------------------
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


-----------------------
Redefinition de l'object qui va definir les class'
------------------------
object stackoverflow {
     |   type Question = Posting
     |   type Answer = Posting
     |   type QID = Int
     |   type HighScore = Int
     |   type LangIndex = Int
     | }
-------------------------



---------------------------
Fcontion de parsing de la data
---------------------------
def rawPostings(lines: RDD[String]): RDD[Posting] =
     |     lines.map(line => {
     |       val arr = line.split(",")
     |       Posting(postingType =    arr(0).toInt,
     |               id =             arr(1).toInt,
     |               acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
     |               parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
     |               score =          arr(4).toInt,
     |               tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
     |     })

--------------------------------

val raw = rawPostings(lines)
===================================
scala> raw.take(20).foreach(println)
Posting(1,27233496,None,None,0,Some(C#))
Posting(1,23698767,None,None,9,Some(C#))
Posting(1,5484340,None,None,0,Some(C#))
Posting(2,5494879,None,Some(5484340),1,None)
Posting(1,9419744,None,None,2,Some(Objective-C))
Posting(1,26875732,None,None,1,Some(C#))
Posting(1,9002525,None,None,2,Some(C++))
Posting(2,9003401,None,Some(9002525),4,None)
Posting(2,9003942,None,Some(9002525),1,None)
Posting(2,9005311,None,Some(9002525),0,None)
Posting(1,5257894,None,None,1,Some(Java))
Posting(1,21984912,None,None,0,Some(Java))
Posting(2,21985273,None,Some(21984912),0,None)
Posting(1,27398936,None,None,0,Some(PHP))
Posting(1,28903923,None,None,0,Some(PHP))
Posting(2,28904080,None,Some(28903923),0,None)
Posting(1,20990204,None,None,6,Some(PHP))
Posting(1,5077978,None,None,-2,Some(Python))
Posting(2,5078493,None,Some(5077978),4,None)
Posting(2,5078743,None,Some(5077978),3,None)
===================================



