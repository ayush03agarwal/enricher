package enricher


trait Mapper[In, Out] extends Serializable {

  def map(input:Iterator[In]): Iterator[Out]
}


