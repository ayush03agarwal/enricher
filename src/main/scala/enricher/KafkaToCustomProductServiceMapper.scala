package enricher

/**
  * Created by ayush.agarwal on 10/05/17.
  */
class KafkaToCustomProductServiceMapper extends Mapper[KafkaPayload, CustomProductServiceResponse] {

  override def map(input: Iterator[KafkaPayload]): Iterator[CustomProductServiceResponse] = {
    Iterator(CustomProductServiceResponse("name-2", "address-2"))
  }
}


case class CustomProductServiceResponse(name: String, address: String)