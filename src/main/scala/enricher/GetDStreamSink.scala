package enricher

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by ayush.agarwal on 10/05/17.
  */
class GetDStreamSink(dstream: DStream[_]) {

  def getDStreamSink(scc: StreamingContext, sinkType: Sink): DStream[_] = {
    sinkType match {
      case kafka: KafkaSink =>
//        val source = KafkaDStreamSink(kafka.config, kafka.topics)
//        source.createSource(scc)
        null
      case _ => println("Not implemented")
        null
    }
  }

}
