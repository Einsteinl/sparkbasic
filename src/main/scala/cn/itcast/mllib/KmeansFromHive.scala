package cn.itcast.mllib

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016-5-29.
  * create table tbl_stock(
  * orderid string,
  * orderlocation string,
  * dateid string
  * )
  * row format delimited
  * fields terminated by ","
  * lines terminated by "\n";
  *
  *
  * create table tbl_stockdetail(
  * orderid string,
  * itmenum string,
  * itemid string,
  * itemqty int,
  * itemprice int,
  * itemamout int
  * )
  * row format delimited
  * fields terminated by ","
  * lines terminated by "\n";
  *
  */
object KmeansFromHive {

  def main(args: Array[String]) {

    LogLevelSetter.setLevel()

    //  如果在windows本地跑，需要从widnows访问HDFS，需要指定一个合法的身份
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("kmeans")

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    hiveContext.sql("set spark.sql.shuffle.partitions=1")  //默认shuffle分区数是20个

    //先从hive中加载到日志数据
    hiveContext.sql("use mllib")
    //查询每家店的商品总数和总金额
    val data = hiveContext.sql("select a.orderlocation, sum(b.itemqty) totalqty,sum(b.itemamout) totalamount from tbl_stock a join tbl_stockdetail b on a.orderid=b.orderid group by a.orderlocation")
    /*data.collect().foreach(x => {
      println(x)
    })*/

    //将hive中查询过来的数据，每一条变成一个向量，整个数据集就变成矩阵
    val parsedata = data.map{
      case Row(_,totalqty,totalamount) =>
        val features = Array[Double](totalqty.toString.toDouble,totalamount.toString.toDouble)
        //  将数组变成机器学习中的向量
        Vectors.dense(features)
    }

    //用kmeans对样本向量进行训练得到模型
    //聚成3类
    val numcluster = 3

    val maxIterations = 20   //指定最大迭代次数
    //聚类最后还用于分类，一开始有大堆样本没有标签，聚完之后就有了标签，以后就可以做分类
    //train方法后返回一个聚类模型，里边存着中心点
    val model = KMeans.train(parsedata,numcluster,maxIterations)

    //用模型对我们到数据进行预测
    val resrdd = data.map{

      case Row(orderlocation,totalqty,totalamount) =>
        //提取到每一行到特征值
        val features = Array[Double](totalqty.toString.toDouble,totalamount.toString.toDouble)
        //将特征值转换成特征向量
        val linevector = Vectors.dense(features)
        //将向量输入model中进行预测，得到预测值
        val prediction = model.predict(linevector)

        //返回每一行结果String，订单，商品总量，商品总金额，预测类别
        orderlocation + " " + totalqty + " " + totalamount + " " + prediction

    }

    resrdd.collect().foreach(x=>{
      println(x)
    })
    // resrdd.saveAsTextFile("/mllib/kmeans/")

    sc.stop()

  }

}
