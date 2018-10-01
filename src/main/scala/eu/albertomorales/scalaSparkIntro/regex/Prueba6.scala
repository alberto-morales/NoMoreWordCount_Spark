package eu.albertomorales.scalaSparkIntro.regex

object Prueba6 {
  
    def doIt() {
      val cadena = "/hphis/ping.png"
      val HCISEstaticosURLPattern = "^\\S+(\\.js|\\.gif|\\.css|\\.jpg|\\.png)$".r
      try {     
        val HCISEstaticosURLPattern(archivo) = cadena    
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