package eu.albertomorales.scalaSparkIntro.FirstExample

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkException

object FirstExample {
  
  def main(args: Array[String]): Unit = {
    
    try { 
      val conf = new SparkConf().setAppName("FirstExample").setMaster("spark://bigdata:7077");      
      val sc = new SparkContext(conf)
      val textFile = sc.textFile("hdfs://bigdata:9000/data/spark-in/README.md")     
      val nLineas = textFile.count()
      println("Las lineas son: "+nLineas)
      
      val textFile2 = sc.textFile("hdfs://bigdata:9000/data/spark-in/words.txt")
      val rDelMap = textFile2.map(_.split(" "))
      val con = rDelMap.collect()
      val tiene = con.length
      println(tiene)
      val rDelFlatMap = textFile2.flatMap(_.split(" "))
      val con2 = rDelFlatMap.collect()
      val tiene2 = con2.length
      println(tiene2)
    } catch {
      case se : SparkException => se.printStackTrace()
    }
  }
  
}