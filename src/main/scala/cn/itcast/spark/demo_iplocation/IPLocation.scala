package cn.itcast.spark.demo_iplocation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**知识点：广播变量，将一个较小的数据集副本发送到application运行的每个节点上，将其保留下来。后续的计算可以直接引入计算
  *               没有广播变量的情况下，每次用到较小的数据集时，都会重新计算，并通过网络传入
  *
  * 过程：
  * 1.读入ip.txt,切分为 (startNum,endNum,province)
  * 2.将上边所计算完毕的rdd1，广播为广播变量
  * 3.读入access.log 将ip抽取出来
  * 4.遍历每个ip，根据广播变量匹配省份province
  * 5.对省份进行聚合，算出在某个时间区间中，各个省份访问网站有多少人
  */
object IPLocation {

  //ip转成整数
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //二分查找
  def binarySearch(lines: Array[(String,String,String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName("IPLocation").setMaster("local[2]")
    val sc=new SparkContext(conf)

    val ipRulesRdd=sc.textFile("c://ip.txt").map(line =>{
      val fileds=line.split("\\|")
      val start_num=fileds(2)
      val end_num=fileds(3)
      val province =fileds(6)
      (start_num,end_num,province)
    })

    //全部的ip规则
    val ipRulesArray=ipRulesRdd.collect()
    //println(ipRulesArray.toBuffer)

    //广播规则
    val ipRulesBroadcast =sc.broadcast(ipRulesArray)

    //加载要处理的数据
    val ipsRDD=sc.textFile("c://access.log").map(line =>{
      val fields =line.split("\\|")
      fields(1)
    })

    val result=ipsRDD.map(ip =>{
      val ipNum=ip2Long(ip)
      val index=binarySearch(ipRulesBroadcast.value,ipNum)
      val info=ipRulesBroadcast.value(index)
      info
    }).map(x =>(x._3,1)).reduceByKey(_+_)

    println(result.collect().toBuffer)

    sc.stop()
  }

}
