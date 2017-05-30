package enricher

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.flipkart.spark.KafkaDStreamSource

/**
  * Created by ayush.agarwal on 10/05/17.
  */
class GetDStreamSource {

  def getDStreamSource(scc: StreamingContext, sourceType: Source): DStream[_] = {
    sourceType match {
      case kafka: KafkaSource =>
        val source = KafkaDStreamSource(kafka.config, kafka.topics)
        source.createSource(scc)
      case _ => println("Not implemented")
        null
    }
  }

}
