package cn.itcast.spark.demo02

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 根据日志统计出每个用户在站点所呆时间最长的前2个的信息
  *1.先根据“手机号_站点”为唯一标识，算一次进站出站的时间，返回(手机号_站点，时间间隔)
  * 2.以“手机号_站点”为key，统计每个站点的时间总和，（“手机号_站点”，时间总和）
  * 3.("手机号_站点"，时间总和)--> (手机号，站点，时间总和)
  * 4.(手机号，站点，时间总和) --> groupBy().mapValues(以时间排序，取出前2个)-->(手机 -> ((m,s,t)(m,s,t)))
  */
object UserLocation {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UserLocation").setMaster("local")

    val sc=new SparkContext(conf)
//(18688888888_16030401EAFB68F1E3CDF819735E1C66,-20160327082400), (18611132889_16030401EAFB68F1E3CDF819735E1C66,-20160327082500)
    val mobileBsTime=sc.textFile("c://bs_log").map(line =>{
      val fields=line.split(",")
      val eventType=fields(3)
      val time =fields(1)
      val timeLong= if(eventType=="1") -time.toLong else time.toLong
      (fields(0)+"_"+fields(2),timeLong)
    })
    //println(mobileBsTime.collect().toBuffer)
    //接下来，对手机和和基站groupby，时间相加
//(18611132889_9F36407EAD0629FC166F14DDE7970F68,54000), (18688888888_9F36407EAD0629FC166F14DDE7970F68,51200)
    val mobileBsTimeSum=mobileBsTime.groupBy(_._1).mapValues(_.foldLeft(0L)(_+_._2))
    //println(mobileBsTimeSum.collect().toBuffer)

    //对手机和基站进行拆分，形成手机，基站，所呆时间
    val mobile_lac_time=mobileBsTimeSum.map(t =>{
      val mobile_bs=t._1
      val mobile=mobile_bs.split("_")(0)
      val lac=mobile_bs.split("_")(1)
      val time=t._2
      (mobile,lac,time)
    })



    //对手机号进行分组，mapvalues进行内部排序

    val mobile_lac_timeGroupMoblie=mobile_lac_time.groupBy(_._1)

    val result=mobile_lac_timeGroupMoblie.mapValues(it =>{
      it.toList.sortBy(_._3).reverse.take(2)
    })

    println(result.collect().toBuffer)

    sc.stop()
  }

}
