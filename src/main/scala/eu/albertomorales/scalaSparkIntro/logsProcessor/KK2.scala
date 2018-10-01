package eu.albertomorales.scalaSparkIntro.FirstExample

import eu.albertomorales.scalaSparkIntro.logsProcessor.RequestStatistics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object KK2 {
  
  val HCISLogPattern = "^(?!#)(\\S+\\s+\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+\\??\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(.+)\\s+(\\S+)\\s+(\\S+)$".r

  val HCISAjaxServletURLPattern = "^\\S+AjaxServlet\\.servl\\S+classtoexecute=([^&]+)\\S*$".r
  val HCISImprimirEscritosURLPattern = "^\\S+imprimirEscritos.do\\S+escrito=(\\d+)\\S*".r
  val HCISGenericURLPattern = "^/hphis/([^\\?]+)\\?\\S+".r
  val HCISPlantillaURLPattern = "^\\S+/edoctor/tmp/plantilla(\\d+)\\.html$".r
  val HCISEstaticosURLPattern = "^\\S+(\\.js|\\.gif|\\.css|\\.jpg|\\.png)$".r
  
  def ahora(): String = {
    //Get current date time
    val now: LocalDateTime = LocalDateTime.now()
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val formatDateTime: String = now.format(formatter)
    formatDateTime
  }
  
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
  
  def updateFunction(nuevosValores: Seq[Double], runningRequestStatistics: Option[RequestStatistics]): Option[RequestStatistics] = {
    // si exite runningRequestStatistics utilizo ese, si no creo un new RequestStatistics
    val oldRequestStatistics: RequestStatistics = runningRequestStatistics.getOrElse(new RequestStatistics())
    val newRequestStatistics = oldRequestStatistics.compute(nuevosValores)
    Some(newRequestStatistics)
  }
  
  private def startModifyFilesTimeThread(logsPath: String, hadoopURI: String) {
    new Thread("Schedule Files Time Modification") {
      override def run() { modifyFilesTime(logsPath, hadoopURI) }
    }.start()
  }

  private def modifyFilesTime(logsPath: String, hadoopURI: String) {
    println("thread tambien EStarted")
    Thread.sleep(5000)
    println("slip finalizado")
    val timeForFiles = System.currentTimeMillis()
    val hdfsConfiguration = new Configuration();
    hdfsConfiguration.set("fs.defaultFS", hadoopURI)
    val hdfsFS = FileSystem.get(hdfsConfiguration)
    hdfsFS.listStatus( new Path(logsPath) ).foreach( file => hdfsFS.setTimes(file.getPath, timeForFiles, timeForFiles) )
    println("terminado el listStatus")
  }
  
  def main(args: Array[String]): Unit = {
    
    // val logsPath: String = "hdfs://bigdata:9000/data/logs-spark"
    // val logsPath: String = "hdfs://bigdata:9000/data/logs-in"
    
    // val hadoopURI = "hdfs://bigdata:9000/"
    
    val sparkURI: String = args(0)
    val logsPath: String = args(1)
    val hadoopURI: String = args(2)
    val checkpointDirectory: String = args(3)
    
    val sparkConf = new SparkConf().setAppName("LogsProcessor").setMaster(sparkURI) // "spark://bigdata:7077"
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir(checkpointDirectory)
    val ssc = new StreamingContext(sc,  Seconds(900)) // 120

    val textFileStream = ssc.textFileStream(logsPath)     
    val mapsDStream = textFileStream.map(linea => {
        try {     
          val HCISLogPattern(g_fecha_hora, g_tiempo, g_l1, g_metodo, g_uri, g_codigo, g_tamanio, g_agente, g_l2, g_nodo) = linea
          val uriDefinitiva = mapearURI(g_uri)
          (uriDefinitiva, g_tiempo.toDouble)
        } catch {
          case e: scala.MatchError => {
            println("***** ERROR (MatchError) ***** linea='"+linea+"'")
            e.printStackTrace()
            ("", -1.0)
          }
          case _: Throwable => {
            println("***** ERROR (Throwable) ***** linea='"+linea+"'")
            ("", -1.0)
          }
        }             
    })
    mapsDStream.persist(StorageLevel.DISK_ONLY)
    val accessLogStatisticsDStream = mapsDStream.updateStateByKey(updateFunction)
    accessLogStatisticsDStream.persist(StorageLevel.DISK_ONLY)
    accessLogStatisticsDStream.foreachRDD { rdd => 
      println("VA un collect ;-) "+ahora())
      rdd.checkpoint()
      rdd.collect().foreach(a => println(a))
      println("FIN del collect :-( "+ahora())
    }
    
    println("before start "+ahora())
    ssc.start()
    println("started "+ahora())
    startModifyFilesTimeThread(logsPath, hadoopURI)

    ssc.awaitTermination()
    println("terminated "+ahora())
    
  }
  
}