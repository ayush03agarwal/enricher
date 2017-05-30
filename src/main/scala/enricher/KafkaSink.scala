package enricher

/**
  * Created by ayush.agarwal on 09/05/17.
  */
case class KafkaSink(config: Map[String, String], topic: String) extends Sink
