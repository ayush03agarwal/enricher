package org.flipkart.utility

import java.io.InputStream
import java.lang.reflect.{ParameterizedType, Type => JType}

import com.fasterxml.jackson.core.JsonParser.Feature
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._
import scala.reflect.{ClassTag, _}

object JsonUtility {

  private[this] val className = this.getClass.getCanonicalName

  val logger = LoggerFactory.getLogger(className)

  val currentMirror = runtimeMirror(getClass.getClassLoader)

  val objectMapper = getObjectMapperWithConfigure

  val tolerantObjectMapper = getObjectMapperWithConfigure
  tolerantObjectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)

  private def getObjectMapperWithConfigure: ObjectMapper = {
    val localObjectMapper = new ObjectMapper() with ScalaObjectMapper
    localObjectMapper.registerModules(Seq(DefaultScalaModule): _*)
//    localObjectMapper.registerModule(new GuavaModule)
    localObjectMapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
    localObjectMapper
  }

  def getMapper: ObjectMapper = {
    objectMapper
  }

  implicit class JSONMarshallFunctions(val o: AnyRef) {
    def toJson = objectMapper.writeValueAsString(o)

    def toJsonWithoutFailure = tolerantObjectMapper.writeValueAsString(o)

    def toJsonBytes = objectMapper.writeValueAsBytes(o)
  }

  implicit class JSONUnMarshallFromMapFunctions(val mapVal: Map[String, Any]) {

    def convertToObject[T: ClassTag]: Option[T] = {
      if (BaseUtility.isNullOrEmpty(mapVal))
        return None

      try {
        Some(objectMapper.convertValue(mapVal, classTag[T].runtimeClass).asInstanceOf[T])
      } catch {
        case e: Exception =>
          logger.error(s"DESERIALIZATION_FAILED.REASON:${e.getMessage}", e)
          None
      }
    }

    def convertToObject(implicit cType: Class[_]) = objectMapper.convertValue(mapVal, cType)

    def convertToObject[T](tTag: TypeTag[T]): Option[T] = {
      try {
        Some(objectMapper.convertValue[T](mapVal, typeReference[T](tTag)).asInstanceOf[T])
      } catch {
        case e: Exception =>
          logger.error(s"DESERIALIZATION_FAILED.REASON:${e.getMessage}", e)
          None
      }
    }
  }


  implicit class JSONUnMarshallFunctions(val string: String) {

    def getObject[T: ClassTag]: Option[T] = {
      if (BaseUtility.isNullOrEmpty(string))
        return None

      try {
        Some(objectMapper.readValue(string, classTag[T].runtimeClass).asInstanceOf[T])
      } catch {
        case e: Exception =>
          logger.error(s"DESERIALIZATION_FAILED.REASON:${e.getMessage}", e)
          None
      }
    }

    def getObject(implicit cType: Class[_]) = objectMapper.readValue(string, cType)

    def getObject[T](tTag: TypeTag[T]): Option[T] = {
      try {
        Some(objectMapper.readValue[T](string, typeReference[T](tTag)).asInstanceOf[T])
      } catch {
        case e: Exception =>
          logger.error(s"DESERIALIZATION_FAILED.REASON:${e.getMessage}", e)
          None
      }
    }
  }

  implicit class JSONISUnMarshallFunctions(val inputStream: InputStream) {

    def getObject[T: ClassTag]: Option[T] = {
      try {
        Some(objectMapper.readValue(inputStream, classTag[T].runtimeClass).asInstanceOf[T])
      } catch {
        case e: Throwable =>
          logger.error(s"DESERIALIZATION_FAILED.REASON:${e.getMessage}", e)
          None
      }
    }

    def getObjectWithTypeTag[T](tTag: TypeTag[T]): Option[T] = {
      try {
        Some(objectMapper.readValue[T](inputStream, typeReference[T](tTag)).asInstanceOf[T])
      }
      catch {
        case e: Exception =>
          logger.error(s"DESERIALIZATION_FAILED.REASON:${e.getMessage}", e)
          None
      }
    }
  }

  implicit class JSONBytesUnMarshallFunctions(val bytes: Array[Byte]) {
    def getObject[T: ClassTag]: Option[T] = {
      try {
        Some(objectMapper.readValue(bytes, classTag[T].runtimeClass).asInstanceOf[T])
      } catch {
        case e: Throwable =>
          logger.error(s"DESERIALIZATION_FAILED.REASON:${e.getMessage}", e)
          None
      }
    }

    def getObjectWithTypeTag[T](tTag: TypeTag[T]) = {
      objectMapper.readValue[T](bytes, typeReference[T](tTag)).asInstanceOf[T]
    }
  }

  private def typeReference[T](tag: TypeTag[T]): TypeReference[_] = new TypeReference[T] {
    override val getType = jTypeFromType(tag.tpe)
  }

  private def jTypeFromType(tpe: Type): JType = {
    val typeArgs = tpe match {
      case TypeRef(_, _, args) => args
    }
    val runtimeClass = currentMirror.runtimeClass(tpe)
    if (typeArgs.isEmpty) {
      runtimeClass
    }
    else new ParameterizedType {
      def getRawType = runtimeClass

      def getActualTypeArguments = typeArgs.map(jTypeFromType).toArray

      def getOwnerType = runtimeClass.getEnclosingClass
    }
  }
}
