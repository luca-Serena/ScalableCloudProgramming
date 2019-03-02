import State._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._
import scala.collection.mutable


object MyDbscan {
  var eps = 0.25
  var minPts = 3
  var clusters :mutable.Set[mutable.Set[Record]] = mutable.Set()


  def main(sc: SparkContext, records: RDD[Record]) {
    DbScan(records)
    clusters.foreach(x => x.foreach(y =>  println(x.size + " size cluster: " + y.weighsVector.toString())))
  }

  def DbScan(records: RDD[Record]): Unit = {
    records.foreach{
      x => if (x.state == Unvisited) {execute(x, records)}
    }
  }

  def execute (rec: Record, records: RDD[Record]): Unit ={
    rec.state = Visited
    rec.neighbors= Some(getNeighbors(rec, records))
    if (rec.neighbors.get.count() < minPts){
      rec.state = Noise
    } else {
      var centroid = mutable.Set(rec)
      rec.state= ClusterMember
      clusters += centroid
      rec.neighbors.get.foreach { x => expand(x, rec, centroid, records)}
    }
  }

  def expand (newRecord: Record, centroid: Record, cluster: mutable.Set[Record], records: RDD[Record]): Unit ={
    if (newRecord.state != ClusterMember){
      newRecord.state = Visited
      newRecord.neighbors = Some (getNeighbors(newRecord, records))
      if (newRecord.neighbors.get.count() >= minPts){
        centroid.neighbors.get++newRecord.neighbors.get
      }
    }
    if (newRecord.state != ClusterMember){
      cluster += newRecord
      newRecord.state = ClusterMember
    }
  }

  def calculateDistance (record1 : Record, record2: Record): Double = {
    val temp = record1.weighsVector.toArray.union(record2.weighsVector.toSeq).map(_._1)
    val first = temp.map(record1.weighsVector)
    val second = temp.map(record2.weighsVector)
    return CosineSimilarity.cosineSimilarity(first,second)
  }

  def getNeighbors (rec: Record, records: RDD[Record]): RDD[Record] ={
    return records.filter(calculateDistance(_, rec) > eps)
  }
}
