package enricher

/**
  * Created by ayush.agarwal on 09/05/17.
  */
case class KafkaSource(config: Map[String, String], topics: Set[String]) extends Source
