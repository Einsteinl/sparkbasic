package cn.itcast.gamedemo.computekpi

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算游戏的各种指标
  */
object GameKPI {
  def main(args: Array[String]): Unit = {
    val queryTime="2016-02-02 00:00:00"
    //计算日志的开始日期
    val beginTime=TimeUtils(queryTime)
    //计算日志的截止日期
    val endTime=TimeUtils.getCertainDayTime(+1)
    val conf=new SparkConf().setAppName("GameKPI").setMaster("local[*]")
    val sc=new SparkContext(conf)

    //将日志先按照时间进行过滤
    //将每一行数据切分
    //(1,2016年2月1日,星期一,10:01:08,10.51.4.168,李明克星,法师,男,1,0,0/800000000)
    val splitedLogs=sc.textFile("c://GameLog.txt").map(_.split("\\|"))
    //按照日期过滤数据并缓存
    val filteredLogs=splitedLogs.filter(fields => FilterUtils.filterByTime(fields,beginTime,endTime))
      .cache()

    //日新增用户 Daily New Users 缩写 DNU
    val dnu=filteredLogs.filter(fields => FilterUtils.filterByType(fields,EventType.REGISTER)).count()

    println(dnu)

    //日活跃用户数 DAU (Daily Active Users)
    val dau=filteredLogs.filter(fields => FilterUtils.filterByTypes(fields,EventType.REGISTER,EventType.LOGIN))
      .map(_(3))
      .distinct()
      .count()
    println(dau)

    //留存率：某段事件的新增用户数记为A，经过一段时间后，仍然使用的用户占新增用户A的比例即为留存率
    //次日留存率 (Day 1 Retention Ratio) Retention Ratio
    //日新增用户在+1日登陆的用户占新增用户的比例

    //获取昨天的时间
    val t1=TimeUtils.getCertainDayTime(-1)
    //过滤出昨天注册的用户
    val lastDayRegUser=splitedLogs.filter(fields => FilterUtils.filterByTypeAndTime(fields,EventType.REGISTER,t1,beginTime))
      .map(x =>(x(3),1))

    //过滤出今天登录的用户
    val todayLoginUser=filteredLogs.filter(fields => FilterUtils.filterByType(fields,EventType.LOGIN))
      .map(x =>(x(3),1))
      .distinct()

    //用昨天注册的用户join今天登录的用户，得出昨天注册并且在今天登录的用户
    val dlr: Double=lastDayRegUser.join(todayLoginUser).count()
    println(dlr)
    //用昨天注册且今天登录的人数除以昨天注册的人数得出留存率
    val dlrr=dlr/lastDayRegUser.count()

    println(dlrr)

    sc.stop()

  }


}
