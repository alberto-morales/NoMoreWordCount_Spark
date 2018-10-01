package eu.albertomorales.scalaSparkIntro.regex

object Prueba4 {
    def comprueba(cadena : String) {
   // val HCISGenericURLPattern = "^/hphis/(\\S+)\\?\\S+([^(&)]+)".r
      val HCISGenericURLPattern = "^/hphis/([^\\?]+)\\?\\S+".r
      try {     
        // val HCISGenericURLPattern(base, argumentos) = cadena   
        val HCISGenericURLPattern(base) = cadena    
        println("Es una urlGenerica: "+base)
      } catch {
        case e: scala.MatchError => {
          println("No es una urlGenerica, o es una urlGenerica SIN argumentos (que tb vale)")
        }
        case _: Throwable => {
          println("Error inesperado")
        }
      }
    }
    def doIt() {
		  val cadena1 = "/hphis/adt/controlFlujoUrgDerivacion/mostrarPantallaAltaUrg.adt?numerohc=2693084&amp;conexion=CCE001&amp;episodio=1042353805&amp;llegadaInmediata=false&amp;pacienteIncompleto=true"
      comprueba(cadena1)
		  val cadena2 = "/hphis/edoctor/PanelDatosPaciente.jsp?numerohc=1323170&amp;conexion=CH0089&amp;codpaciente=1323170&amp;requiereCrearNuevo=N&amp;frameSuperior=SI&amp;accion=seleccionPrincipal"
      comprueba(cadena2)
		  val cadena3 = "/hphis/edoctor/PanelDatosPaciente.jsp?param=3"
      comprueba(cadena3)
      val cadena4 = "/hphis/edoctor/PanelDatosPaciente.jsp?numerohc=2285697&amp;conexion=CH0089&amp;codpaciente=2285697&amp;requiereCrearNuevo=N&amp;frameSuperior=SI&amp;accion=seleccionPrincipal?pantalla=PantallaSeleccion&amp;numerohc=2285697&amp;conexion=CH0089&amp;iup="
      comprueba(cadena4)
      val cadena5 = "/hphis/edoctor/PanelDatosPaciente.jsp"
      comprueba(cadena5)
    }
      
    def main(args: Array[String]) {
      doIt()
    }
    
}