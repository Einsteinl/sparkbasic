package cn.itcast.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

/**
 * Created by Administrator on 2015-11-30.
 */
object LogLevelSetter  extends Logging{

  def setLevel(){
    val isInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements()
    if(!isInitialized){
      logInfo("seting log level to warn------")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}