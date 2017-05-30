package org.flipkart.utility

import java.io._
import java.net.URI
import java.security.MessageDigest
import java.sql.Timestamp
import java.util.Date

import org.apache.commons.lang3.StringUtils
import org.flipkart.utility.JsonUtility.JSONMarshallFunctions
import org.slf4j.LoggerFactory

import scala.Predef._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.sys.process._
import scala.util.{Failure, Random, Success, Try}

object Utility {

  private[this] val className = this.getClass.getCanonicalName

  val logger = LoggerFactory.getLogger(className)

  val targetExpressionIdLength: Int = 12

  implicit class ConvertToTryFunctions[T](val optionalObject: Option[T]) {
    def toTry: Try[T] = optionalObject match {
      case Some(response) => Success(response)
      case None => Failure(new Exception("UNEXPECTED_NULL_RESPONSE"))
    }
  }

  implicit class ObjectHandyFunction(val obj: AnyRef) {
    def asStringMap: Map[String, Any] = {
      val fieldsAsPairs = for (field <- obj.getClass.getDeclaredFields) yield {
        field.setAccessible(true)
        val fieldValue = field.get(obj) match {
          case date: Date => DateUtility.toDbString(date)
          case enum: Enumeration#Value => enum.toString
          case v => v
        }
        (field.getName, fieldValue)
      }
      Map(fieldsAsPairs: _*)
    }
  }

  implicit class PartionFunctionsOnList[T](val list: List[T]) {

    def untilFailure(p: T => Try[Unit]): Option[Throwable] = untilFailure(list, p)

    def untilFailureException(p: T => Try[Unit]): Try[Unit] = {
      untilFailure(list, p) match {
        case Some(e) => Failure(e)
        case None => Success(Unit)
      }
    }

    private def untilFailure(lst: List[T], p: T => Try[Unit]): Option[Throwable] = {
      if (lst.nonEmpty) {
        p(lst.head) match {
          case Failure(e) => Some(e)
          case Success(x) => untilFailure(lst.tail, p)
        }
      }
      else None
    }
  }

  implicit class ProcessStringFunctions(val string: String) {
    def generateSalt(len: Int = 4): String = {
      md5String(string).substring(0, len)
    }
  }

  /**
    * Function to retry the caller function n number of times with the defined delay
    */
  def doRetry[T](numOfRetries: Int, initialSleep: Int = 100)(body: => T): T = {
    var retry = 0
    var sleep = initialSleep
    var myError: Throwable = null
    while (retry < numOfRetries) {
      try {
        return body
      } catch {
        case e: Throwable =>
          myError = e
          retry += 1
          sleep *= 2
          Thread.sleep(sleep.toLong)
      }
    }
    throw new Throwable("RETRY_FAILED", myError)
  }

  def isNotNullAndNonEmpty(o: Any): Boolean = !isNullOrEmpty(o)

  def isNullOrEmpty(o: Any): Boolean = o match {
    case m: Map[_, _] => m.isEmpty
    case i: Iterable[Any] => i.isEmpty
    case null | None | StringUtils.EMPTY => true
    case Some(x) => isNullOrEmpty(x)
    case _ => false
  }

  def getDetail(obj: Any, path: String, splitter: String = "/"): Option[Any] = {
    var myObj = obj
    val parts = path.split(splitter)
    var i = 0
    do {
      var index = parts(i)
      var negated = false
      if (parts(i).startsWith("~")) {
        index = parts(i).split("~").tail.head
        negated = true
      }
      val value = negated match {
        case true => myObj match {
          case _: Map[_, _] => myObj.asInstanceOf[Map[String, Any]].filter(_._1 != index)
          case _: List[_] =>
            if (myObj.asInstanceOf[List[Any]].nonEmpty)
              myObj.asInstanceOf[List[Any]] diff List[Any](myObj.asInstanceOf[List[Any]].get(index.toInt))
            else
              return None
          case _: java.util.Map[_, _] => myObj.asInstanceOf[java.util.Map[String, AnyRef]].filter(_._1 != index)
          case _: java.util.List[_] =>
            if (myObj.asInstanceOf[java.util.List[Any]].nonEmpty)
              myObj.asInstanceOf[java.util.List[Any]] diff List[Any](myObj.asInstanceOf[java.util.List[Any]].get(index.toInt))
            else
              return None
          case _ => return None
        }
        case false => myObj match {
          case _: Map[_, _] => myObj.asInstanceOf[Map[String, Any]].get(index)
          case _: List[_] =>
            if (myObj.asInstanceOf[List[Any]].length > index.toInt)
              myObj.asInstanceOf[List[Any]].get(index.toInt)
            else
              return None
          case _: java.util.Map[_, _] => myObj.asInstanceOf[java.util.Map[String, AnyRef]].get(index)
          case _: java.util.List[_] =>
            if (myObj.asInstanceOf[java.util.List[Any]].length >= index.toInt)
              myObj.asInstanceOf[java.util.List[Any]].get(index.toInt)
            else
              return None
          case _ => return None
        }
      }
      myObj = value match {
        case Some(null) => return None
        case Some(x) => x
        case Nil => return None
        case null => return None
        case None => return None
        case _ => value
      }
      i = i + 1
    } while (i < parts.size)
    Some(myObj)
  }

  def convertJavaObjectToScala(obj: Any): Any = {
    obj match {
      case _: java.util.Map[_, _] =>
        val originalMap = obj.asInstanceOf[java.util.Map[String, AnyRef]].asScala
        var newMap = Map[String, Any]()
        originalMap.foreach(row => newMap += row._1 -> convertJavaObjectToScala(row._2))
        newMap
      case _: java.util.List[_] => obj.asInstanceOf[java.util.List[String]].asScala.toList
      case _: Map[_, _] =>
        val originalMap = obj.asInstanceOf[Map[String, AnyRef]]
        var newMap = Map[String, Any]()
        originalMap.foreach(row => newMap += row._1 -> convertJavaObjectToScala(row._2))
        newMap
      case _ => obj
    }
  }

  def getSystemEnv(param: String, defaultValue: String): String = {
    System.getenv(param) match {
      case null => defaultValue
      case value => value
    }
  }

  def isValidAccountId(accountId: String): Boolean = accountId.toUpperCase.startsWith("AC") && (accountId.length > 10)

  def isNotTaintedDeviceId(deviceId: String): Boolean = {
    !deviceId.toUpperCase.startsWith("T_D") || !deviceId.toUpperCase.startsWith("PERF")
  }

  def isValidDeviceId(deviceId: String): Boolean = {
    !StringUtils.containsAny(deviceId, " ") && !deviceId.equalsIgnoreCase("null") && deviceId.nonEmpty
  }

  def isNotTaintedMobileNo(mobileNo: String): Boolean = {
    !mobileNo.startsWith("0000")
  }

  def md5String(s: String): String = MessageDigest.getInstance("MD5").digest(s.getBytes).map("%02x".format(_)).mkString

  def generateTargetExpressionId: String = "TE" + generateRandomStr(targetExpressionIdLength)

  def generateRandomStr(len: Int): String = {
    val ZERO = Character.valueOf('0')
    val A = Character.valueOf('A')
    val sb = new StringBuilder
    for (i <- 1 to len) {
      var n = (36.0 * Math.random).asInstanceOf[Int]
      if (n < 10) {
        n = ZERO + n
      } else {
        n -= 10
        n = A + n
      }
      sb.append(new Character(n.asInstanceOf[Char]))
    }
    sb.toString()
  }

  def generateRandomHexStr(len: Int): String = {
    val sb = new StringBuffer()
    for (i <- 1 to len) {
      sb.append(Integer.toHexString(new Random().nextInt(16)))
    }
    sb.toString
  }

  def trimString(str: String, len: Int): String = {
    if (str.length > len)
      str.take(len - 3) + "..."
    else
      str
  }

  def generateTGResultReference = generateRandomHexStr(6)

  def randomInt(range: Int = 100): Int = Random.nextInt(range)

  def generateStandardAPIResponse(code: Int, request: Map[String, Any], response: Map[String, Any]): (Int, String) = {
    var resMap = Map[String, Any]()
    resMap += "REQUEST" -> request
    resMap += "STATUS" -> code.toString
    resMap += "RESPONSE" -> response
    Tuple2(code, resMap.toJson)
  }

  def getCompleteUrl(url: String, baseUrl: String): Try[String] = {
    Try {
      val uri = new URI(url.toString)
      val finalUrl = if (uri.getHost == null) baseUrl + uri.toString else uri.toString
      finalUrl
    }
  }

  def containsAttribute(dataMap: Any, key: String, splitter: String = "/"): Boolean = {
    val opt = Utility.getDetail(dataMap, key, splitter)
    opt match {
      case Some(x) => true
      case None => false
    }
  }

  def sanitizeFileName(name: String) = {
    name.replaceAll("[^a-zA-Z0-9-_\\.]", "_")
  }

  def writeToFile(fileName: String, data: String): Unit = {
    val out: BufferedWriter = new BufferedWriter(new FileWriter(new File(fileName)), 32768)
    out.write(data)
    out.close()
  }

  def writeToFile(fileName: String, data: Array[Byte]): Unit = {
    val out: OutputStream = new FileOutputStream(fileName)
    out.write(data)
    out.close()
  }

  def runProcess(command: String): String = {
    command.!!
  }

  def now(): Timestamp = {
    new Timestamp(new Date().getTime)
  }

  def numDayToName(num: Int): String = {
    num match {
      case 0 => "Everyday"
      case 1 => "Monday"
      case 2 => "Tuesday"
      case 3 => "Wednesday"
      case 4 => "Thursday"
      case 5 => "Friday"
      case 6 => "Saturday"
      case 7 => "Sunday"
      case _ => "N/A"
    }
  }

  def cleanNumber(number: String): String = {
    number.replaceAll("[^0-9]", "").takeRight(10).toString
  }

}
