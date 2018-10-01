package eu.albertomorales.scalaSparkIntro.logsProcessor

import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import scala.collection.JavaConverters._

@SerialVersionUID(100L)
class RequestStatistics(listaValores : Seq[Double]) extends Serializable {

    private var valoresJ: java.util.List[Double] = new java.util.ArrayList[Double]()
    
    for (nuevoValor <- listaValores) {
      if (nuevoValor > -1.0) {
        valoresJ.add(nuevoValor)
      }
    }

    var arrayTiempos = valoresJ.asScala.toArray
    private val count = valoresJ.size()
		private val percentile90 = new Percentile().evaluate(arrayTiempos, 90);
		private val min = new Min().evaluate(arrayTiempos);
		private val max = new Max().evaluate(arrayTiempos);    
    
		// con estas 2 lineas jodemos el windowing, pero lo hacemos para liberar memoria 
		// y porque una vez probado, no necesitamos el windowing, va todo en 1 (UNA) ventana.
		// ini - ñapa gorda e inválida
		valoresJ = new java.util.ArrayList[Double]()
		arrayTiempos = valoresJ.asScala.toArray
		// fin - ñapa gorda e inválida
		
    def this() {
      this(List())  
    }
    
    def compute(valuesToCompute: Seq[Double]): RequestStatistics = {
       val oldValues = arrayTiempos.toSeq
       val resultado: RequestStatistics = new RequestStatistics(valuesToCompute ++ oldValues)
       resultado
    }
    
    override def toString = {
      ""+ count + " " + percentile90 + " " + min + " " + max
    }

}