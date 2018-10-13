package cn.itcast.spark.demo_sort

import org.apache.spark.{SparkConf, SparkContext}

object OrderContext{
  // 隐式的声明了Girl的增强对象Ordering[Girl]
  implicit val girlOrdering =new Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue > y.faceValue) 1
      else if(x.faceValue==y.faceValue){
        if(x.age > y.age) -1 else 1
      } else -1
    }
  }
}


/**
  * sort =>规则 先按faceValue，再比较年龄
  * name，faceValue，age
  *
  * 说明：RDD排序通常指定Tuple中的某一个字段，这里指定了一个实例，按照自定义的方法进行排序
  */
object CustomSort {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    val sc=new SparkContext(conf)

    val rdd1=sc.parallelize(List(("yuihatano",90,28,1),("angelababy",90,27,2),("JuJingYi",95,22,3)))

    //第一种方式
    //val rdd2=rdd1.sortBy(x => Girl(x._2,x._3),false)

    //第二种方式
    import cn.itcast.spark.demo_sort.OrderContext._
    val rdd2=rdd1.sortBy(x => Girl(x._2,x._3),false)
    println(rdd2.collect().toBuffer)
    sc.stop()
  }

}

/**
  * 第一种方式
  * 用了样例类，不用new对象，可以直接使用（单例对象）
  */
//case class Girl(val faceValue: Int,val age: Int) extends Ordered[Girl] with Serializable{
//  override def compare(that: Girl): Int = {
//    if(this.faceValue==that.faceValue){
//      that.age-this.age
//    }else{
//      this.faceValue-that.faceValue
//    }
//  }
//}

/**
  *第二种，通过隐式转换完成排序
  */
case class Girl(faceValue: Int,age: Int) extends Serializable