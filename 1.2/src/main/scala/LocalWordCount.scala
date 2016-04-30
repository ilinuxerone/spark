/**
  * Created by Administrator on 2016/4/30.
  */
package cn.hw.streaming

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.StreamingContext._
//import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.log4j.Level

object LocalWordCount {
  def main(args : Array[String]) :Unit = {
    if (args.length < 3){
      System.err.println("Usage: LocalWordCount <master> <inputFile> <outputFile>")
      System.exit(1)
    }
    val inputFile = args(1)
    val outputFile = args(2)

    StreamingExamples.setStreamingLogLevels(Level.WARN)

    //Initialize StreamingContext
/*    val ssc = new StreamingContext(args(0), "HdfsWordCount", Seconds(args(2).toInt),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))*/

    var conf = new SparkConf().setMaster("local").setAppName("WordCountDemo")
    var sc = new SparkContext(conf)

    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile(outputFile)
  }
}
