package eu.albertomorales.scalaSparkIntro.logsProcessor.pruebas.regex

import eu.albertomorales.scalaSparkIntro.logsProcessor.HCISLogPattern._

object Prueba {
    def comprueba(cadena : String) {
      try {     
        val LinePattern(g_fecha_hora, g_tiempo, g_l1, g_metodo, g_uri, g_codigo, g_tamanio, g_agente, g_l2, g_nodo) = cadena    
        println("g_fecha_hora: "+g_fecha_hora)
        println("g_tiempo: "+g_tiempo)
        println("g_l1: "+g_l1)
        println("g_metodo: "+g_metodo)
        println("g_uri: "+g_uri)
        println("g_codigo: "+g_codigo)
        println("g_tamanio: "+g_tamanio)
        println("g_agente: "+g_agente)
        println("g_l2: "+g_l2)
        println("g_nodo: "+g_nodo)
      } catch {
        case e: scala.MatchError => {
          println("No es un log válido")
        }
        case _: Throwable => {
          println("Error inesperado")
        }
      }
    }

    def doIt() {
  		val cadena1 = "2018-07-30	00:18:55	0.003	-	GET	/hphis/axis/HL7Filler	200	130	-	-	prohospital01:7110"
      comprueba(cadena1)
    }
      
    def main(args: Array[String]) {
      doIt()
    }
    
}