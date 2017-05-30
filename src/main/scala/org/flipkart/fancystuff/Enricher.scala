package org.flipkart.fancystuff

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.flipkart.spark.{SparkStreamingApplication, KafkaDStreamSource}

import scala.collection.mutable
import scala.concurrent.duration._


object Enricher extends SparkStreamingApplication {

  def main(args: Array[String]): Unit = {

    val topology = PipeLineConfiguration(
      source = KafkaConfig(brokers = "127.0.0.1:9092", topic = "source_topic"),
      sink = KafkaConfig(brokers = "127.0.0.1:9092", topic = "sink_topic"),
      mappers = Seq(
        new Mapper[SourceObject, SourceObject] {
          def map(in: SourceObject) = in.copy(name = in.name + "-attached")
        },
        new Mapper[SourceObject, SourceObject] {
          def map(in: SourceObject) = in.copy(meta = in.meta ++ Map("age" -> 50, "newdata" -> "lorem-ipsum"))
        },
        new Mapper[SourceObject, OutputObject] {
          def map(in: SourceObject) = OutputObject(name = in.name, age = in.meta("age").asInstanceOf[Int])
        }
      )
    )

    val objects = SourceObject("kinshuk", Map("age" -> 27, "city" -> "bangalore"))

    val conf = new SparkConf()
    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }
    val sc = new SparkContext(conf)

    //    val ssc = new StreamingContext(sc, Seconds(streamingBatchDuration.toSeconds))
    //    withSparkStreamingContext { (sc, ssc) =>
    //      val source: KafkaDStreamSource = _
    val rddSource = sc.parallelize(List(objects, objects.copy(name = "ayush"), objects.copy(name = "girish")), 2) //source.createSource(ssc, "config.inputTopic")

    val stages = new mutable.ArraySeq[RDD[_]](topology.mappers.size)
    topology.mappers.zipWithIndex.foreach { case (mapper, pos) =>
      val out = {
        if (pos == 0) {
          rddSource.map(obj => mapper.asInstanceOf[Mapper[Any, Any]].map(obj))
        }
        else
          stages(pos - 1).map(obj => mapper.asInstanceOf[Mapper[Any, Any]].map(obj))
      }
      stages(pos) = out
    }


    stages.foreach(rdd => rdd.foreach(println))
    //    stages.last.foreach(println)

    //     val ld =  rddSource
    //
    //
    //
    //        .map( x => x.copy(x.name + "-prefix"))
    //          .collect()
    //
    //      ld.foreach(x => println(x))


    //    }

    //    ssc.start()
    //    ssc.awaitTermination()

  }

  override def streamingBatchDuration: FiniteDuration = 2.seconds

  override def streamingCheckpointDir: String = "/tmp/enricher"

  override def sparkConfig: Map[String, String] = Map("spark.master" -> "local[8]", "spark.app.name" -> "Enricher")
}

case class SourceObject(
                         name: String,
                         meta: Map[String, Any]
                       )

case class OutputObject(name: String, age: Int)

trait Source

trait Sink

trait Mapper[In, Out] extends Serializable {
  def map(in: In): Out
}

trait FunctionMapper[In, Out] extends Mapper[In, Out] {
  def className: String

  def map(in: In) = {

    in.asInstanceOf[Out]
  }
}

case class KafkaConfig(brokers: String, topic: String) extends Source with Sink


case class PipeLineConfiguration(source: Source,
                                 mappers: Seq[Mapper[_, _]],
                                 sink: Sink)

