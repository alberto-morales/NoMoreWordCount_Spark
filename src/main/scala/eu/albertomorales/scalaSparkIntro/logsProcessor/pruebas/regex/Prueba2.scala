package eu.albertomorales.scalaSparkIntro.logsProcessor.pruebas.regex

import eu.albertomorales.scalaSparkIntro.logsProcessor.HCISLogPattern._

object Prueba2 {
    def comprueba(cadena : String) {
      try {     
        val AjaxServletURLPattern(accion) = cadena    
        println("Es un ajax: "+accion)
      } catch {
        case e: scala.MatchError => {
          println("No es un ajax")
        }
        case _: Throwable => {
          println("Error inesperado")
        }
      }
    }

    def doIt() {
  		val cadena1 = "/hphis/AjaxServlet.servlet?conexion=CH0089&amp;procedencia=2&amp;centro=2258&amp;modulo=QUI&amp;procedenciaObligatoria=S&amp;classtoexecute=com.hphis.corp.ajax.AjaxComprobarProcedenciaModuloCentro";
      comprueba(cadena1)
  		val cadena2 = "/hphis/AjaxServlet.servlet?classtoexecute=com.hphis.outpatient.ajaxCitacion.AjaxProcedenciaComprobacion&amp;conexion=CH0089&amp;procedencia=2&amp;centroProce=2258&amp;agenteProce=2505&amp;ambito_proce=HOSP&amp;persona=108208&amp;modulo=QUI&amp;activo=&#37;24&#37;7Bparametros.activo&#37;7D&amp;tiporecurs"
      comprueba(cadena2)
  		val cadena3 = "/hphis/AjaxServlet.servlet?conexion=CH0089&amp;classtoexecute=com.hphis.outpatient.ajaxCitacion.AjaxProcedenciaComprobacion&amp;conexion=CH0089&amp;procedencia=5&amp;centroProce=&amp;agenteProce=&amp;ambito_proce=&amp;persona=&amp;modulo=CEX&amp;activo=&#37;24&#37;7Bparametros.activo&#37;7D&amp;tiporecursoProce="
      comprueba(cadena3)
    }
      
    def main(args: Array[String]) {
      doIt()
    }
    
}