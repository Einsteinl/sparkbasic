package cn.itcast.gamedemo.computekpi

import java.text.SimpleDateFormat
import java.util.Calendar

object TimeUtils {

  /**
    * 线程不安全的
    */
  val simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val calendar=Calendar.getInstance()

  //将字符串日期转成Long类型的毫秒数
  def apply(time:String)={
    calendar.setTime(simpleDateFormat.parse(time))
    calendar.getTimeInMillis
  }

  //用来得到24小时后时间的毫秒数
  def getCertainDayTime(amount: Int): Long={
    calendar.add(Calendar.DATE,amount)
    val time=calendar.getTimeInMillis
    calendar.add(Calendar.DATE,-amount)
    time
  }

}
