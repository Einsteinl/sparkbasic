package cn.itcast.sparkstreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 以DStream中的数据进行按key做reduce操作，然后对各个批次的数据进行累加
  */
object StateFulWordCount {

  //分好组的数据
  //Seq这个批次某个单词的次数
  //Option[Int] 以前的结果
  val updateFunc=(iter: Iterator[(String,Seq[Int],Option[Int])]) =>{

//    iter.flatMap(it=>Some(it._2.sum+it._3.getOrElse(0)).map(x =>(it._1,x)))
//    iter.map{case(x,y,z) =>Some(y.sum+z.getOrElse(0)).map(m =>(x,m))}
//    iter.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))

    iter.map{case(word,current_count,history_count) =>(word,current_count.sum + history_count.getOrElse(0))}


  }

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()

    val conf=new SparkConf().setAppName("StateFulWordCount").setMaster("local[2]")

    val sc=new SparkContext(conf)
    //updateStateByKey必须设置setCheckpointDir
    sc.setCheckpointDir("c://ck")

    val ssc=new StreamingContext(sc,Seconds(5))

    val ds=ssc.socketTextStream("192.168.150.11",8888)

    /**
      * updateStateByKey操作，可以让每个key维护一份state，并持续不断的更新该state。
      * 1、首先，要定义一个state，可以是任意的数据类型；
      * 2、其次，要定义state更新函数——指定一个函数如何使用之前的state和新值来更新state。
      * 对于每个batch，Spark都会为每个之前已经存在的key去应用一次state更新函数，无论这个key在batch中是否有新的数据。如果state更新函数返回none，那么key对应的state就会被删除。
      * 当然，对于每个新出现的key，也会执行state更新函数。
      * 注意，updateStateByKey操作，要求必须开启Checkpoint机制。
      */
    //定义状态更新函数：用一个函数指定怎样使用先前的状态。从输入流中的新值更新状态。
    val result=ds.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),true)


    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
