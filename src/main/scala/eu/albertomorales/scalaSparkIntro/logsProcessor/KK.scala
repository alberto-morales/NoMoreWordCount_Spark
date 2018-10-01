package eu.albertomorales.scalaSparkIntro.FirstExample

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KK {

  val HCISLogPattern = "^(?!#)(\\S+\\s+\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+\\??\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(.+)\\s+(\\S+)\\s+(\\S+)$".r

  val HCISAjaxServletURLPattern = "^\\S+AjaxServlet\\.servl\\S+classtoexecute=([^&]+)\\S*$".r
  val HCISImprimirEscritosURLPattern = "^\\S+imprimirEscritos.do\\S+escrito=(\\d+)\\S*".r
  val HCISGenericURLPattern = "^/hphis/([^\\?]+)\\?\\S+".r
  val HCISPlantillaURLPattern = "^\\S+/edoctor/tmp/plantilla(\\d+)\\.html$".r
  val HCISEstaticosURLPattern = "^\\S+(\\.js|\\.gif|\\.css|\\.jpg|\\.png)$".r
  
  def mapearURI(uri: String): String = {
      try {     
        val HCISEstaticosURLPattern(extension) = uri    
        return "*** ESTATICO [.gif|.js|.css|.jpg|.png] ***"
      } catch {
        case e: scala.MatchError => {
        }
        case _: Throwable => {
        }
      }  
      try {     
        val HCISAjaxServletURLPattern(accion) = uri    
        return "/hphis/AjaxServlet.servlet&classtoexecute=" + accion
      } catch {
        case e: scala.MatchError => {
        }
        case _: Throwable => {
        }
      }
      try {     
        val HCISImprimirEscritosURLPattern(escrito) = uri    
        return "/hphis/imprimirEscritos.do?escrito=" + escrito
      } catch {
        case e: scala.MatchError => {
        }
        case _: Throwable => {
        }
      }          
      try {     
        val HCISPlantillaURLPattern(plantilla) = uri    
        return "/hphis/edoctor/tmp/plantilla" + plantilla + ".html"
      } catch {
        case e: scala.MatchError => {
        }
        case _: Throwable => {
        }
      }             
      try {     
        val HCISGenericURLPattern(base) = uri    
        return "/hphis/" + base
      } catch {
        case e: scala.MatchError => {
        }
        case _: Throwable => {
        }
      }                  
      return uri    
  }
  
  def parsearLinea(linea: String): String = {
    val HCISLogPattern(g_fecha_hora, g_tiempo, g_l1, g_metodo, g_uri, g_codigo, g_tamanio, g_agente, g_l2, g_nodo) = linea
    return mapearURI(g_uri)
  }
  
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("FirstStreamingExample").setMaster("spark://bigdata:7077");
    val sc = new SparkContext(sparkConf)
    // val ssc = new StreamingContext(sc,  Seconds(30))
    val ssc = new StreamingContext(sc,  Seconds(120))

    val textFileStream = ssc.textFileStream("hdfs://bigdata:9000/data/logs-spark")     
    val urlCountsDStream = textFileStream.map(linea => {
        try {     
          val uriDefinitiva = parsearLinea(linea)
          (uriDefinitiva, 1)
        } catch {
          case e: scala.MatchError => {
            ("", 0)
          }
          case _: Throwable => {
            ("", 0)
          }
        }             
    // }).reduceByKey((accum, n) => (accum + n))
    }).reduceByKeyAndWindow((accum, n) => (accum + n), Seconds(120))
    urlCountsDStream.foreachRDD { rdd => 
      rdd.collect().foreach(a => println(a))
    }
    
    println("before start")
    ssc.start()
    println("started")
    ssc.awaitTermination()
    println("terminated")
    
  }
  
}