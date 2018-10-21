package cn.itcast.gamedemo.computekpi

import org.apache.commons.lang3.time.FastDateFormat


object FilterUtils {

  val dateFormat=FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")

  //对fields进行判断是否在startTime和endTime之间
  def filterByTime(fields:Array[String],startTime:Long,endTime:Long):Boolean={
    val time=fields(1)
    val logTime=dateFormat.parse(time).getTime
    logTime >= startTime && logTime<endTime
  }

  //按照类型进行过滤，过滤出与eventType一样的数据
  def filterByType(fields: Array[String],eventType: String) :Boolean={
    val _type=fields(0)
    eventType==_type
  }
  //根据多个事件类型进行过滤，只要符合一个事件就返回true
  def filterByTypes(fields: Array[String],eventTypes: String*): Boolean={
    val _type=fields(0)
    for(et <- eventTypes){
      if(_type == et)
        return true
    }
    false
  }

  //过滤出在某个时间段发生了evenType事件的数据
  def filterByTypeAndTime(fields: Array[String],eventType: String,beginTime: Long,endTime:Long): Boolean={
    val _type=fields(0)
    val _time=fields(1)
    val logTime=dateFormat.parse(_time).getTime
    eventType==_type && logTime >= beginTime && logTime < endTime
  }

}
