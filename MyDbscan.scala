import State._

import scala.collection.mutable
import scala.io.Source


object MyDbscan {
  var eps = 0.2
  var minPts = 3
  var centroids :mutable.Set [mutable.Set[Record]] = mutable.Set()


  var record1 = new Record ("", Map("drug" -> 0.3, "female" -> 0.3, "weed" -> 0.19, "camping" -> 0.1) )
  var record2 = new Record ("", Map("Trump" -> 0.35, "commerce" -> 0.2, "usa" -> 0.2))
  var record3 = new Record ("", Map("usa" -> 0.2, "nba" -> 0.2))
  var record4 = new Record ("", Map("nba" -> 0.3, "mvp" -> 0.2, "usa" -> 0.2))
  var record5 = new Record ("", Map("weed" -> 0.29, "legalize" -> 0.5, "therapy" -> 0.1))
  var record6 = new Record ("", Map("kitchen" -> 0.3, "female" -> 0.3, "io" -> 0.1))
  var record7 = new Record ("", Map("orca" -> 0.3, "commerce" -> 0.2, "india" -> 0.25 ))
  var record8 = new Record ("", Map("male" -> 0.32, "female" -> 0.3, "angry" -> 0.27))
  var record9 = new Record ("", Map("torino" -> 0.3, "city" -> 0.2, "io" -> 0.1, "party" -> 0.19))
  var record10 = new Record ("", Map("me" -> 0.15, "freestyle" -> 0.2, "funny" -> 0.15))
  var record11 = new Record ("", Map("ecstasy" -> 0.37, "drug" -> 0.3, "me" -> 0.15))
  var record12 = new Record ("", Map("male" -> 0.3, "female" -> 0.3 , "wife" -> 0.5))
  var record13 = new Record ("", Map("milano" -> 0.3, "party" -> 0.2))
  var record14 = new Record ("", Map("usa" -> 0.3, "death" -> 0.2, "penalty" -> 0.5))
  var record15 = new Record ("", Map("nfl" -> 0.5, "usa" -> 0.2, "me" -> 0.15))
  var record16 = new Record ("", Map("me" -> 0.15, "io" -> 0.2, "selfish" -> 0.4))
  var record17 = new Record ("", Map("Trump" -> 0.3, "economy" -> 0.21, "global" -> 0.19))
  var record18 = new Record ("", Map("weed" -> 0.29, "smoke" -> 0.23, "drug" -> 0.45))
  var record19 = new Record ("", Map("minghie" -> 0.3, "fee" -> 0.2,  "one" -> 0.19))
  var record20 = new Record ("", Map("hard" -> 0.3, "fee" -> 0.2,  "party" -> 0.19))
  var record21 = new Record ("", Map("torino" -> 0.3, "gabrio" -> 0.4, "io" -> 0.1, "party" -> 0.19))
  var record22 = new Record ("", Map("economy" -> 0.21, "time" -> 0.25, "progressive" -> 0.4, "develop" -> 0.19))
  val records: Set [Record] = Set (record1, record2, record3, record4, record5, record6, record7, record8, record9, record10,
    record11, record12, record13, record14, record15, record16, record17, record18, record19, record20, record22, record21)

  def main(args: Array[String]) {
    DbScan()
    centroids.foreach(x => x.foreach(y =>  println(x.size + y.weighsVector.toString()))) /* x.foreach(y => print ("")))) */
  }

  def DbScan( ): Unit = {
    records.foreach{
      x => if (x.state == Unvisited) {execute(x)}
    }
  }

  def execute (rec: Record): Unit = {
    rec.state = Visited
    rec.neighbors = getNeighbors(rec, records)
    if (rec.neighbors.size < minPts) {
      rec.state = Noise
    } else {
      var cluster = mutable.Set(rec)
      rec.state = ClusterMember
      centroids += cluster
      rec.neighbors.foreach { x => expand(x, rec, cluster)}
      }
    }

    def expand(newRecord: Record, centroid: Record, cluster: mutable.Set[Record]): Unit = {
      if (newRecord.state != ClusterMember) {
        newRecord.state = Visited
        newRecord.neighbors = getNeighbors(newRecord, records)
        if (newRecord.neighbors.size >= minPts) {
          //newRecord.neighbors.get.foreach {x => centroid.neighbors.get++ }ù
          centroid.neighbors ++= newRecord.neighbors
        }
      }
      if (newRecord.state != ClusterMember) {
        cluster += newRecord
        newRecord.state = ClusterMember
      }
    }

    def calculateDistance(record1: Record, record2: Record): Double = {
      val temp = record1.weighsVector.toArray.union(record2.weighsVector.toSeq).map(_._1)
      val first = temp.map(record1.weighsVector)
      val second = temp.map(record2.weighsVector)
      return CosineSimilarity.cosineSimilarity(first, second)
    }

    def getNeighbors(rec: Record, records: Set[Record]): Set[Record] = {
      return records.filter(calculateDistance(_, rec) > eps)
    }
}
