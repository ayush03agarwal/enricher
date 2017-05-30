package enricher

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by ayush.agarwal on 09/05/17.
  */
object SimpleTopology {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SimpleTopology")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("/tmp/checkpoint")

    val batchIntervalSeconds = 10

    val topology = Pipeline(
      source = KafkaSource(Map("metadata.broker.list" -> "10.33.97.199:9092,10.33.10.211:9092,10.33.89.206:9092,10.33.77.166:9092,10.33.37.218:9092,10.33.49.208:9092", "auto.offset.reset" -> "largest")
        , Set("price-update-event")),
      mappers = Seq(
        new KafkaToCustomProductServiceMapper
        ,
        new Mapper[CustomProductServiceResponse, KafkaPayload] {
          def map(in: Iterator[CustomProductServiceResponse]): Iterator[KafkaPayload] = {

            val payload =
              """
                |{
                |  "name" : "ayush",
                |  "age" : "27"
                |}
              """.stripMargin
            Iterator(KafkaPayload(Some("23232".getBytes()), "{{\n    \"a\" : \"ayush\"\n    }}".getBytes()))
          }

        }
      ),
      sink = KafkaSink(Map("bootstrap.servers" -> "10.33.205.205:6667,10.33.249.164:6667"), "sink-output")
    )


    // source.
    val getSourceDStream = new GetDStreamSource
    val dStream = getSourceDStream.getDStreamSource(ssc, topology.source)

    dStream.foreachRDD(rddSource => {

      val stages = new mutable.ArraySeq[RDD[_]](topology.mappers.size)
      topology.mappers.zipWithIndex.foreach { case (mapper, pos) =>
        val out = {
          if (pos == 0)
            rddSource.mapPartitions(itr => mapper.asInstanceOf[Mapper[Any, Any]].map(itr))
          else
            stages(pos - 1).mapPartitions(itr => mapper.asInstanceOf[Mapper[Any, Any]].map(itr))
        }
        stages(pos) = out
      }

      stages.foreach(rdd => {
        rdd.foreach(println)
      })

      import org.flipkart.spark.KafkaDStreamSink._
      stages.last.asInstanceOf[RDD[KafkaPayload]].sendToKafka(Map("bootstrap.servers" -> "10.33.205.205:6667,10.33.249.164:6667"), "test-output")

    })

    ssc.start()
    ssc.awaitTermination()


  }
}
