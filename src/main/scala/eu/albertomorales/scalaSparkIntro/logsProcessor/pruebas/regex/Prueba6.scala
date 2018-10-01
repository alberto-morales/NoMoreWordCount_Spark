package eu.albertomorales.scalaSparkIntro.logsProcessor.pruebas.regex

import eu.albertomorales.scalaSparkIntro.logsProcessor.HCISLogPattern._

object Prueba6 {
  
    def doIt() {
      val cadena = "/hphis/ping.png"
      try {     
        val EstaticosURLPattern(archivo) = cadena    
        println("Es estático")
      } catch {
        case e: scala.MatchError => {
          println("No es estático")
        }
        case _: Throwable => {
          println("Error inesperado")
        }
      }
    }
    
    def main(args: Array[String]) {
      doIt()
    }
    
}