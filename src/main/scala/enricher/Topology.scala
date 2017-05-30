package enricher

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by ayush.agarwal on 10/05/17.
  */
trait Topology {

  def topology: Pipeline = ???

  def rddSource: RDD[_] = ???

  def stages = new mutable.ArraySeq[RDD[_]](topology.mappers.size)
//  topology.mappers.zipWithIndex.foreach { case (mapper, pos) =>
//    val out = {
//      if (pos == 0) {
//        rddSource.map(obj => mapper.asInstanceOf[Mapper[Any, Any]].map(obj))
//      }
//      else
//        stages(pos - 1).map(obj => mapper.asInstanceOf[Mapper[Any, Any]].map(obj))
//    }
//    stages(pos) = out
//  }

}
