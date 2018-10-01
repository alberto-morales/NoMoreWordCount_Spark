package eu.albertomorales.scalaSparkIntro.regex

object Prueba3 {
    def comprueba(cadena : String) {
      val HCISImprimirEscritosURLPattern = "^\\S+imprimirEscritos.do\\S+escrito=(\\d+)\\S*".r
      try {     
        val HCISImprimirEscritosURLPattern(escrito) = cadena    
        println("Es un escrito: "+escrito)
      } catch {
        case e: scala.MatchError => {
          println("No es un escrito")
        }
        case _: Throwable => {
          println("Error inesperado")
        }
      }
    }
    def doIt() {
		  val cadena1 = "/hphis/adt/common/imprimirEscritos.do?escrito=52&amp;ncopias=1&amp;conexion=&amp;multiple=&amp;episodio=1042373072&amp;numerohc=3019247&amp;idioma_9=SELECCION_DEFECTO&amp;parametros=centro&#37;3DCH0009&#37;7Cnumerohc&#37;3D3019247&#37;7Cusuario&#37;3Dmicano&#37;7Cconexion&#37;3DCH0009&#37;7Cfgarante&#37;3D60&#37;7Cfservici"
      comprueba(cadena1)
    }
      
    def main(args: Array[String]) {
      doIt()
    }
    
}