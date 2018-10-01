package eu.albertomorales.scalaSparkIntro.regex

object Prueba5 {
    def doIt() {
      val cadena = "/hphis/edoctor/tmp/plantilla1533127959823.html"
      val HCISPlantillaURLPattern = "^\\S+/edoctor/tmp/plantilla(\\d+)\\.html$".r
      try {     
        val HCISPlantillaURLPattern(plantilla) = cadena    
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