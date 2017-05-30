package example

import org.apache.spark.storage.StorageLevel
import org.flipkart.spark.{KafkaDStreamSource, KafkaPayloadStringCodec, SparkStreamingApplication}

import scala.concurrent.duration.FiniteDuration

class GenericJob(config: GenericJobConfig, source: KafkaDStreamSource) extends SparkStreamingApplication {

  override def sparkConfig: Map[String, String] = config.spark

  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckpointDir

  def start(): Unit = {
    withSparkStreamingContext { (sc, ssc) =>
      val input = source.createSource(ssc)

      // Option 1: Array[Byte] -> String
      val stringCodec = sc.broadcast(KafkaPayloadStringCodec())
      val lines = input.flatMap(stringCodec.value.decodeValue)

      val countedWords = WordCount.countWords(
        ssc,
        lines,
        config.stopWords,
        config.windowDuration,
        config.slideDuration
      )

      countedWords.print()



      // Option 2: Array[Byte] -> Specific Avro
      //      val avroSpecificCodec = sc.broadcast(KafkaPayloadAvroSpecificCodec[SomeAvroType]())
      //      val lines = input.flatMap(avroSpecificCodec.value.decodeValue(_))

      // cache to speed-up processing if action fails
      input.persist(StorageLevel.MEMORY_ONLY_SER)

      import org.flipkart.spark.KafkaDStreamSink._
      input.foreachRDD(rdd => rdd.sendToKafka(config.sinkKafka, config.outputTopic))
    }
  }
}


object GenericJob {

  def main(args: Array[String]): Unit = {
    val config = GenericJobConfig()

    val streamingJob = new GenericJob(config, KafkaDStreamSource(config.sourceKafka, Set(config.inputTopic)))
    streamingJob.start()
  }

}

case class GenericJobConfig(
                             inputTopic: String,
                             outputTopic: String,
                             stopWords: Set[String],
                             windowDuration: FiniteDuration,
                             slideDuration: FiniteDuration,
                             spark: Map[String, String],
                             streamingBatchDuration: FiniteDuration,
                             streamingCheckpointDir: String,
                             sourceKafka: Map[String, String],
                             sinkKafka: Map[String, String])
  extends Serializable

object GenericJobConfig {

  import com.typesafe.config.{Config, ConfigFactory}
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader.arbitraryTypeValueReader

  def apply(): GenericJobConfig = apply(ConfigFactory.load)

  def apply(applicationConfig: Config): GenericJobConfig = {

    val config = applicationConfig.getConfig("wordCountJob")

    new GenericJobConfig(
      config.as[String]("input.topic"),
      config.as[String]("output.topic"),
      config.as[Set[String]]("stopWords"),
      config.as[FiniteDuration]("windowDuration"),
      config.as[FiniteDuration]("slideDuration"),
      config.as[Map[String, String]]("spark"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckpointDir"),
      config.as[Map[String, String]]("kafkaSource"),
      config.as[Map[String, String]]("kafkaSink")
    )
  }

}
