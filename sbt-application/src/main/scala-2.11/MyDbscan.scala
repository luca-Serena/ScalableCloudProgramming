import State._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._
import scala.collection.mutable


object MyDbscan {
  var eps = 0.2
  var minPts = 3
  var centroids :scala.collection.mutable.Set[scala.collection.mutable.Set[Record]] = scala.collection.mutable.Set()

  def DbScan(sc: SparkContext, records: RDD[Record]): Unit = {
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
      var centroid = scala.collection.mutable.Set(rec)
      rec.state= ClusterMember
      centroids += centroid
      expandCluster(rec, centroid, records)
    }
  }

  def expandCluster (rec: Record, cluster: scala.collection.mutable.Set[Record], records: RDD[Record]): Unit ={
    rec.neighbors.get.foreach{x => expand(x, records, rec, cluster)}
  }

  def expand (newRecord: Record, records: RDD[Record], centroid: Record, cluster: scala.collection.mutable.Set[Record]): Unit ={
    if (newRecord.state == Unvisited){
      newRecord.state = Visited
      newRecord.neighbors = Some (getNeighbors(newRecord, records))
      if (newRecord.neighbors.get.count() >= minPts){
        //newRecord.neighbors.get.foreach {x => centroid.neighbors.get++ }ù
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
    return records.filter(calculateDistance(_, rec) < eps)
  }
}
