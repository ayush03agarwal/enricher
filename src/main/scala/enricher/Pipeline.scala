package enricher

case class Pipeline(source: Source, mappers: Seq[Mapper[_, _]], sink: Sink)

