// Databricks notebook source
// MAGIC %md ## **Ejemplo básico de Streaming**
// MAGIC 
// MAGIC En este ejemplo revisaremos los rudimentos básicos de Spark Streaming.
// MAGIC 
// MAGIC Para ello recibiremos un stream de líneas de texto del [registro de actividad de servidores web](https://es.wikipedia.org/wiki/Common_Log_Format) en streaming. Por facilidad usaremos un generador en memoria de logs como fuente de datos en streaming.

// COMMAND ----------

// MAGIC %md ## Configuración
// MAGIC 
// MAGIC Las variables que establecemos a continuación nos permitirán hacer un control básico del flujo de la aplicación del presente notebook.

// COMMAND ----------

// === Control de flujo de la aplicación ===
val stopActiveContext = true	 
// "true"  = parar los StreamingContext que ya se estén ejecutando (mejor dejarlo así);       
// "false" = no parar y dejarlos seguir ejecutándose, pero puede ocurrir que los cambios que hagáis en el código no se ejecuten.

// === Configuraciones para Spark Streaming ===
val batchIntervalSeconds = 1 
val eventsPerSecond = 10    // Para el generador en memoria de logs, cuántas líneas de log se generarán por segundo.

// COMMAND ----------

// MAGIC %md ## Importación de librerías
// MAGIC 
// MAGIC Vamos a proceder a importar todas las librerías que vamos a necesitar. Si al ejecutar esta celda se observa algún error habrá que asegurar que se han subido al cluster todas las librarías necesarias. Más información de cómo hacer esto en [la documentación de Databricks](https://docs.databricks.com/user-guide/libraries.html).

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._


// COMMAND ----------

// MAGIC %md ## Setup: Creación del StreamingContext
// MAGIC 
// MAGIC Haremos dos cosas a continuación. 
// MAGIC 1. Definiremos un "custom receiver" de los datos del stream en la clase `DummySource` (ignorad esa parte)
// MAGIC 2. Definiremos una función `creatingFunc()` que crea y devuelve el `StreamingContext`. El StreamingContext es el punto de entrada a la funcionalidad de streaming en Spark. Este StreamingContext encapsula el SparkContext que se utiliza por debajo para procesar los datos. 

// COMMAND ----------

// Este es un Receiver implementado ad hoc para tener una fuente de datos en stream. Se trata de un "generador aleatorio" de logs de acceso web.
// No es relevante entender qué se está haciendo aquí. En el material lectivo comentaremos los Receivers.

import scala.util.Random
import org.apache.spark.streaming.receiver._
import org.apache.spark.streaming.scheduler._

class DummySource(ratePerSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  
  println("creamos el dummysource")
  
  val logs = List(
"109.169.248.247 - - [12/Dec/2015:18:25:11 +0100] \"GET /administrator/ HTTP/1.1\" 200 4263 \"-\" \"Mozilla/5.0 (Windows NT 6.0; rv:34.0) Gecko/20100101 Firefox/34.0\" \"-\"",
"109.169.248.247 - - [12/Dec/2015:18:25:11 +0100] \"POST /administrator/index.php HTTP/1.1\" 200 4494 \"http://almhuette-raith.at/administrator/\" \"Mozilla/5.0 (Windows NT 6.0; rv:34.0) Gecko/20100101 Firefox/34.0\" \"-\"",
"188.45.108.168 - - [12/Dec/2015:19:44:06 +0100] \"GET / HTTP/1.1\" 200 10479 \"http://www.almenland.at/almhuetten-mit-naechtigung.html\" \"Mozilla/5.0 (Linux; Android 4.4.2; de-at; SAMSUNG GT-I9301I Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/1.5 Chrome/28.0.1500.94 Mobile Safari/537.36\" \"-\"",
"188.45.108.168 - - [12/Dec/2015:19:44:06 +0100] \"GET /modules/mod_bowslideshow/tmpl/css/bowslideshow.css HTTP/1.1\" 200 1725 \"http://www.almhuette-raith.at/\" \"Mozilla/5.0 (Linux; Android 4.4.2; de-at; SAMSUNG GT-I9301I Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/1.5 Chrome/28.0.1500.94 Mobile Safari/537.36\" \"-\"",
"188.45.108.168 - - [12/Dec/2015:19:44:06 +0100] \"GET /templates/_system/css/general.css HTTP/1.1\" 404 239 \"http://www.almhuette-raith.at/\" \"Mozilla/5.0 (Linux; Android 4.4.2; de-at; SAMSUNG GT-I9301I Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/1.5 Chrome/28.0.1500.94 Mobile Safari/537.36\" \"-\"",
"188.45.108.168 - - [12/Dec/2015:19:44:07 +0100] \"GET /templates/jp_hotel/css/template.css HTTP/1.1\" 200 10004 \"http://www.almhuette-raith.at/\" \"Mozilla/5.0 (Linux; Android 4.4.2; de-at; SAMSUNG GT-I9301I Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/1.5 Chrome/28.0.1500.94 Mobile Safari/537.36\" \"-\"",
"188.45.108.168 - - [12/Dec/2015:19:44:07 +0100] \"GET /templates/jp_hotel/css/layout.css HTTP/1.1\" 200 1801 \"http://www.almhuette-raith.at/\" \"Mozilla/5.0 (Linux; Android 4.4.2; de-at; SAMSUNG GT-I9301I Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/1.5 Chrome/28.0.1500.94 Mobile Safari/537.36\" \"-\"",
"188.45.108.168 - - [12/Dec/2015:19:44:07 +0100] \"GET /templates/jp_hotel/css/menu.css HTTP/1.1\" 200 1457 \"http://www.almhuette-raith.at/\" \"Mozilla/5.0 (Linux; Android 4.4.2; de-at; SAMSUNG GT-I9301I Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/1.5 Chrome/28.0.1500.94 Mobile Safari/537.36\" \"-\"",
"188.45.108.168 - - [12/Dec/2015:19:44:07 +0100] \"GET /templates/jp_hotel/css/suckerfish.css HTTP/1.1\" 200 3465 \"http://www.almhuette-raith.at/\" \"Mozilla/5.0 (Linux; Android 4.4.2; de-at; SAMSUNG GT-I9301I Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/1.5 Chrome/28.0.1500.94 Mobile Safari/537.36\" \"-\"",
"188.45.108.168 - - [12/Dec/2015:19:44:08 +0100] \"GET /media/system/js/caption.js HTTP/1.1\" 200 1963 \"http://www.almhuette-raith.at/\" \"Mozilla/5.0 (Linux; Android 4.4.2; de-at; SAMSUNG GT-I9301I Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/1.5 Chrome/28.0.1500.94 Mobile Safari/537.36\" \"-\"",
"66.249.69.97 - - [12/Dec/2015:19:44:08 +0100] \"GET /071300/242153 HTTP/1.1\" 404 514 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\" \"-\"",
"188.45.108.168 - - [12/Dec/2015:19:44:09 +0100] \"GET /nf HTTP/1.1\" 404 505 \"-\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36\" \"-\"",
"188.45.108.168 - - [12/Dec/2015:19:44:09 +0100] \"GET /plain.php HTTP/1.1\" 500 505 \"-\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36\" \"-\"")

  def onStart() {
    new Thread("Logs Dummy Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
  }

  private def receive() {
    while(!isStopped()) {      
      store(logs(Random.nextInt(logs.size)))
      Thread.sleep((1000.toDouble / ratePerSec).toInt)
    }
  }
}

// COMMAND ----------

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.log4j.{Level, Logger}

class MyStreamingListener extends StreamingListener {
  
  println("creamos el listener")
  
  private val LOG: Log = LogFactory.getLog(this.getClass)
    
  override def onStreamingStarted(streamingStarted : StreamingListenerStreamingStarted) {
    LOG.debug("onStreamingStarted") 
    println("onStreamingStarted")  
  }
  
  override def onReceiverStarted(receiverStarted : StreamingListenerReceiverStarted) {
    LOG.debug("onReceiverStarted") 
    println("onReceiverStarted") 
  } 
  
  override def onReceiverError(receiverError : StreamingListenerReceiverError) {
    LOG.debug("onReceiverError") 
    println("onReceiverError") 
  }
  
  override def onReceiverStopped(receiverStopped : StreamingListenerReceiverStopped) {
    LOG.debug("onReceiverStopped") 
    println("onReceiverStopped") 
  }
  
  override def onBatchSubmitted(batchSubmitted : StreamingListenerBatchSubmitted) {
    LOG.debug("onBatchSubmitted") 
    println("onBatchSubmitted") 
  }
  
  override def onBatchStarted(batchStarted : StreamingListenerBatchStarted) {
    LOG.debug("onBatchStarted") 
    println("onBatchStarted") 
  }
  
  override def onBatchCompleted(batchCompleted : StreamingListenerBatchCompleted) {
    LOG.debug("onBatchCompleted") 
    println("onBatchCompleted") 
  }
  
  override def onOutputOperationStarted(outputOperationStarted : StreamingListenerOutputOperationStarted) {
    LOG.debug("onOutputOperationStarted") 
    println("onOutputOperationStarted") 
  }
  
  override def onOutputOperationCompleted(outputOperationCompleted : StreamingListenerOutputOperationCompleted) {
    LOG.debug("onOutputOperationCompleted") 
    println("onOutputOperationCompleted") 
  }
  
}


// COMMAND ----------

  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter

  def ahora(): String = {
    //Get current date time
    val now: LocalDateTime = LocalDateTime.now()
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val formatDateTime: String = now.format(formatter)
    formatDateTime
  }

// COMMAND ----------

var newContextCreated = false      // Flag para detectar si se ha creado un nuevo contexto o no

// Función de creación y configuración de un nuevo StreamingContext
def creatingFunc(): StreamingContext = {
  
  // creamos un StreamingListener
  val myListener : StreamingListener = new MyStreamingListener()
  // Creamos un StreamingContext con un tamaño de microbatch de batchIntervalSeconds
  val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
  // agregamos el listener (StreamingListener)
  println("agregamos el listener")
  ssc.addStreamingListener(myListener)
  
  val contador = sc.longAccumulator("contador")
  
  // Creamos un stream en el que recibiremos las líneas de log de nuestro generador dummy.
  // Este es el punto en el que conectamos Spark Streaming con la fuente de datos en stream.
  println("vamos a arrancar el dummysource?")
  val lines = ssc.receiverStream(new DummySource(eventsPerSecond))
  // Podríamos haber creado el stream sobre un socket en un puerto, sobre un fichero de HDFS, sobre un tópico de Spark...
  
  // Aprovechamos para definir el procesamiento que vamos a hacer de los datos que nos lleguen por el stream.
  // Filtraremos solo aquellas líneas que incluyan un código de respuesta HTTP 500 (un error en el servidor)
  val errorLines = lines.filter(_.contains(" 500 "))
  
  /*
  val lineas = lines.map(linea => {
     contador.add(1)
     linea
  })
  */

  // lineas.print()
  
   lines.foreachRDD { rdd => 
      println("VA un collect ;-) "+ahora())
      rdd.collect().foreach(a => {
        contador.add(1) 
        print(".")
      })
      println("FIN del collect :-( "+ahora())
      println("el contador.value es "+contador.value)
    }
  
  println("Se ha invocado la función para crear un nuevo StreamingContext")
  newContextCreated = true  
  
  ssc
}

// COMMAND ----------

// MAGIC %md ## Arrancamos los trabajos de streaming
// MAGIC 
// MAGIC El código además para cualquier StreamingContext ya existente y comienza/reinicia uno nuevo. En ese punto se usan las variables del comienzo de notebook para decidir si parar un contexto y comenzar uno nuevo o recuperar uno anterior.

// COMMAND ----------

if (stopActiveContext) {
  // Paramos cualquier StreamingContext activo 
  StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
} 

// Obtenemos o creamos el StreamingContext con el que trabajar
val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
if (newContextCreated) {
  println("Nuevo contexto creado desde la función creatingFunc") 
} else {
  println("Hay un contexto ejecutándose o se ha recuperado de un checkpoint")
}

println("before start "+ahora())
// Arrancamos el StreamingContext en background. A partir de la invocación a start() se comenzarán a recibir datos del stream.
ssc.start()
println("started "+ahora())
// Desde este momento comenzamos a recibir los datos del stream. Spark Streaming comienza a planificar jobs en el SparkContext encapsulado por el StreamingContext.
// Esto va a ocurrir en un hilo separado así que para mantener activa nuestra aplicación (que no termine o finalice) vamos a invocar a continuación a awaitTermination()
// o awaitTerminationOrTimeout() para esperar a la finalización de la computación sobre el stream.

// ssc.awaitTermination()
ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 20 * 1000) // Esto pondrá esta celda del notebook en espera durante 20 veces el batchIntervalSeconds.
println("terminated "+ahora())
// ssc.awaitTermination() // No lo vamos a ejecutar pues dejaría esta celda bloqueada a la espera de finalizar el procesamiento del stream, que tal como se ha desarrollado el dummy reciever no dejará de emitir líneas nunca.


// COMMAND ----------

// MAGIC %md 
// MAGIC Es importante entender en este punto un par de cosas:
// MAGIC 1. El StreamingContext solo se puede arrancar una vez
// MAGIC 2. El StreamingContext deberá arrancar después de que hayamos configurado los DStreams y las operaciones que vamos a realizar sobre ellos

// COMMAND ----------

// MAGIC %md ### Finalizar el StreamingContext
// MAGIC 
// MAGIC Si quisiéramos finalizar el StreamingContext basta con descomentar la línea y ejecutar la celda.
// MAGIC 
// MAGIC `StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }`

// COMMAND ----------

// StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
