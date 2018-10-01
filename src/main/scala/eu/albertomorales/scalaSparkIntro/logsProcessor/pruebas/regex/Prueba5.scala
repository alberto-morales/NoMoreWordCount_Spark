package eu.albertomorales.scalaSparkIntro.logsProcessor.pruebas.regex

import eu.albertomorales.scalaSparkIntro.logsProcessor.HCISLogPattern._

object Prueba5 {
    def doIt() {
      val cadena = "/hphis/edoctor/tmp/plantilla1533127959823.html"
      try {     
        val PlantillaURLPattern(plantilla) = cadena    
        println("Es una plantilla: "+plantilla)
      } catch {
        case e: scala.MatchError => {
          println("No es una plantilla")
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