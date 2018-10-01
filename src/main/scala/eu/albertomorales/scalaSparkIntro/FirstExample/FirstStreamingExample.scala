package eu.albertomorales.scalaSparkIntro.FirstExample

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FirstStreamingExample {
  
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("FirstStreamingExample").setMaster("spark://bigdata:7077");
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,  Seconds(30))

    val textFileStream = ssc.textFileStream("hdfs://bigdata:9000/data/logs-spark")     
    val lineCountDStream = textFileStream.count()
    lineCountDStream.print()
    
    println("before start")
    ssc.start()
    println("started")
    ssc.awaitTermination()
    println("terminated")
    
  }
  
}