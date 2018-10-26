package cn.itcast.gamedemo

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object ElasticSpark {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("ElasticSpark").setMaster("local[*]")
    conf.set("es.nodes","mini5,mini6,mini7")
    conf.set("es.port","9200")
    conf.set("es.index.auto.create","true")

    val sc =new SparkContext(conf)
   // val query: String="{\"query\":{\"match_all\":{}}}"
    val start = 1461340800  //2016/4/23 00:00:00
    val end = 1461427200    //2016/4/24 00:00:00
    //    val query: String =
    //      s"""{
    //       "query": {"match_all": {}},
    //       "filter": {
    //         "bool": {
    //           "must": {
    //             "range": {
    //               "access.time": {
    //                 "gte": "$start",
    //                 "lte": "$end"
    //               }
    //             }
    //           }
    //         }
    //       }
    //     }"""
    val tp = "1"
    val query: String = s"""{
       "query": {"match_all": {}},
       "filter" : {
          "bool": {
            "must": [
                {"term" : {"track.type" : $tp}},
                {
                "range": {
                  "track.time": {
                  "gte": "$start",
                  "lte": "$end"
                  }
                }
              }
            ]
          }
       }
     }"""

    //拉取elasticSearch中index为tracklog的数据
    val rdd1 = sc.esRDD("tracklog", query)

    //println(rdd1.collect().toBuffer)
    println(rdd1.collect().size)

    /*
    求网站的有效用户，访问了网站（事件类型1）并且打开了登录器（事件类型2）
     */
    val rdd2=rdd1.map(x =>{
      val message=x._2

    })
    println(rdd2.collect().toBuffer)

  }

}
