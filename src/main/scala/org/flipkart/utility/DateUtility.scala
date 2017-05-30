package org.flipkart.utility

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, Date, TimeZone}

import org.slf4j.LoggerFactory

object DateUtility {

  def sdf = new SimpleDateFormat("yyyy-MM-dd") /* SDF is not thread-safe. Never make it val */

  private [this] val className = this.getClass.getCanonicalName

  val logger = LoggerFactory.getLogger(className)

  def toDateFromDatetime(dateString: String,inputFormat:String="yyyy-MM-dd HH:mm:ss"): Date = {
    try {
      val sdf = new SimpleDateFormat(inputFormat)
      sdf.setTimeZone(TimeZone.getTimeZone("Asia/Calcutta"))
      sdf.parse(dateString)
    } catch {
      case e: Exception =>
        logger.error("toDateFromDateTime:" + e.getMessage, e)
        null
    }
  }

  def getCutOffDate:Date = {
    new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").parse("12/31/2050 23:59:59")
  }

  def epochToUIString(epochTime: Long, dateFormat: String): String = {
    new SimpleDateFormat(dateFormat).format(epochTime)
  }

  def toDbString(date: Date): String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)
  }

  def getStartDate(date:Date): String = {
    new SimpleDateFormat("yyyy-MM-dd 00:00:00").format(date)
  }

  def getISODateString(date: Date = new Date(System.currentTimeMillis)): String = {
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+05:30'").format(date)
  }

  def getISODateString(date: String): String = {
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+05:30'").format(toDateFromDatetime(date))
  }

  def getDateForES(date: String): String = {
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(toDateFromDatetime(date))
  }

  def getWeekRangeDbDates(d: Date): Map[String, String] = {
    val range = getWeekRange(d)
    Map("start" -> toDbString(range("start").getTime), "end" -> toDbString(range("end").getTime))
  }

  def getWeekRange(d: Date): Map[String, Calendar] = {
    var range = Map[String, Calendar]()
    val today: Calendar = Calendar.getInstance
    val start = today.clone.asInstanceOf[Calendar]
    val end = today.clone.asInstanceOf[Calendar]
    today.setTime(d)
    start.setTime(d)
    end.setTime(d)
    val f: Int = today.get(Calendar.DAY_OF_WEEK)
    start.add(Calendar.DAY_OF_WEEK, 1 - f)
    start.set(Calendar.HOUR_OF_DAY, 0)
    start.set(Calendar.MINUTE, 0)
    start.set(Calendar.SECOND, 0)
    end.add(Calendar.DAY_OF_WEEK, 7 - f)
    end.set(Calendar.HOUR_OF_DAY, 23)
    end.set(Calendar.MINUTE, 59)
    end.set(Calendar.SECOND, 59)
    range += "start" -> start
    range += "end" -> end
    range
  }

  def weekDayJavaToCeryx(dayOfWeek: Int): Int = dayOfWeek match {
    case 1 => 7
    case _ => dayOfWeek - 1
  }

  def weekDayCeryxToJava(dayOfWeek: Int): Int = dayOfWeek match {
    case 7 => 1
    case _ => dayOfWeek + 1
  }

  def getCurrentDate: String = sdf.format(new Date())


  def getCurrentDateTime: String =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())

  def getYesterdayDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis - 24 * 60 * 60 * 1000))

  def getStartEpochInSec(date: String): Long = {
    val calendar = Calendar.getInstance
    calendar.setTime(sdf.parse(date))
    calendar.getTimeInMillis / 1000
  }

  def getEndEpochInSec(date: String): Long = {
    val calendar = Calendar.getInstance
    calendar.setTime(sdf.parse(date))
    calendar.set(Calendar.HOUR, 23)
    calendar.set(Calendar.MINUTE, 59)
    calendar.set(Calendar.SECOND, 59)
    calendar.getTimeInMillis / 1000
  }


  def epochToDateTimeMinuteSecPrecision(epochTime: Long): String = {
    //converts epoch time to IST (upto HH:mm:ss)
    val date: Date = new Date(epochTime * 1000)
    val format: DateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
    format.setTimeZone(TimeZone.getTimeZone("Asia/Calcutta"))
    format.format(date)
  }

  def epochInMillsToDateTimeMinuteSecPrecision(epochTime: Long): String = {
    //converts epoch time to IST (upto HH:mm:ss)
    val date: Date = new Date(epochTime)
    val format: DateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
    format.setTimeZone(TimeZone.getTimeZone("Asia/Calcutta"))
    format.format(date)
  }

  def dateTimeMinuteSecPrecisionToDate(IST: String): Date = {
    //converts time (upto HH:mm:ss) in String to date
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.parse(IST)
  }

  def getDateNotifyMeHbase(date: Date = new Date()): String = new SimpleDateFormat("yyyyMMdd").format(date)

  def getNotifyMePrefixDate(dateString: String): String = {
    val date = toDateFromDatetime(dateString)
    new SimpleDateFormat("yyyyMMddHHmmss").format(date)
  }

  def getYesterdayNotifyMeHbase(date: Date = new Date()): String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    new SimpleDateFormat("yyyyMMdd").format(cal.getTime)
  }

  def getElapsedSec(date: Date): Long = {
    (new Date().getTime - date.getTime) / 1000
  }

  def getShiftedDateTime(shiftValue: Int, timeUnit: Int = Calendar.DATE, date: Date = new Date(), withTime: Boolean = true): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(timeUnit, shiftValue)
    if (withTime)
      toDbString(cal.getTime)
    else
      sdf.format(cal.getTime)
  }

  def getShiftedDateTimeFromToday(shiftValue: Int): Date = {
    new Date(System.currentTimeMillis() - (shiftValue*86400*1000L))
  }


  def getDateWithZeroTime(date: Date): Date = {
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.set(Calendar.HOUR_OF_DAY,0)
    calendar.set(Calendar.MINUTE,0)
    calendar.set(Calendar.SECOND,0)
    calendar.set(Calendar.MILLISECOND,0)
    calendar.getTime
  }

  def getOnlyTimeInMilliSecondsFromDate(date: Date): Long = {
    date.getTime - getDateWithZeroTime(date).getTime
  }

  def getOnlyTimeInMilliSecondsFromDate(dateInEpoch: Long): Long = getOnlyTimeInMilliSecondsFromDate(new Date(dateInEpoch))

  def getStartOfWeek: Date = {
    val weekCal = Calendar.getInstance
    weekCal.setTimeInMillis(System.currentTimeMillis)
    weekCal.setFirstDayOfWeek(Calendar.MONDAY)
    weekCal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    weekCal.set(Calendar.HOUR_OF_DAY, 0)
    weekCal.set(Calendar.MINUTE, 0)
    weekCal.set(Calendar.SECOND,0)
    weekCal.getTime
  }

  def getStartOfDay: Date = {
    val todayCal = Calendar.getInstance
    todayCal.setTimeInMillis(System.currentTimeMillis)
    todayCal.set(Calendar.HOUR_OF_DAY,0)
    todayCal.set(Calendar.MINUTE,0)
    todayCal.set(Calendar.SECOND,0)
    todayCal.getTime
  }

  def getCurrentTimeInHex : String = {
    new Date().getTime.toHexString
  }

  def getFirstDayOfWeek(date: Date): Date ={
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    val dayOfWeek: Int = calendar.get(Calendar.DAY_OF_WEEK) - calendar.getFirstDayOfWeek
    calendar.add(Calendar.DAY_OF_MONTH, -dayOfWeek)
    calendar.getTime
  }

  def getFirstDayOfMonth(date: Date): Date ={
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    calendar.getTime
  }
}