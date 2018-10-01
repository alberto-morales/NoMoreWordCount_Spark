package eu.albertomorales.scalaSparkIntro.logsProcessor

object HCISLogPattern {
  
  final val LinePattern = "^(?!#)(\\S+\\s+\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+\\??\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(.+)\\s+(\\S+)\\s+(\\S+)$".r

  final val AjaxServletURLPattern = "^\\S+AjaxServlet\\.servl\\S+classtoexecute=([^&]+)\\S*$".r
  final val ServletCexCitaURLPattern = "^\\S+ServletCexCita\\.servlet\\S+accion=([^&]+)\\S*$".r  
  final val ImprimirEscritosURLPattern = "^\\S+imprimirEscritos.do\\S+escrito=(\\d+)\\S*".r
  final val GenericURLPattern = "^/hphis/([^\\?]+)\\?\\S+".r
  final val PlantillaURLPattern = "^\\S+/edoctor/tmp/plantilla(\\d+)\\.html$".r
  final val EstaticosURLPattern = "^\\S+(\\.js|\\.gif|\\.css|\\.jpg|\\.png)$".r
  
}