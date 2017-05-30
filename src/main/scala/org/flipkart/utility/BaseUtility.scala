package org.flipkart.utility

import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ArrayBuffer

object BaseUtility {

  def isNullOrEmpty(o: Any): Boolean = o match {
    case m: Map[_, _] => m.isEmpty
    case i: Iterable[Any] => i.isEmpty
    case null | None | StringUtils.EMPTY => true
    case Some(x) => isNullOrEmpty(x)
    case _ => false
  }

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

  def string2list[T](listString:String):ArrayBuffer[T] = {
    val result :ArrayBuffer[T] = new ArrayBuffer[T]()
    listString.split(",").filter(!_.equalsIgnoreCase("")).foreach(x => result += x.asInstanceOf[T])
    result
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
}
