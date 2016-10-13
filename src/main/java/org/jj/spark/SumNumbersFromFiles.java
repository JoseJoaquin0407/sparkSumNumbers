package org.jj.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

public class SumNumbersFromFiles 
{
    public static void main( String[] args )
    {
    	//Paso 1: Acceder a Spark y configurarlo
    	SparkConf conf= new SparkConf();
    	//Paso 2: Creamos el contexto a partir de la configuración 
    	JavaSparkContext context=new JavaSparkContext(conf);
    	
    	//Version simplificada
    	int sum=context.textFile(args[0]).map(s->Integer.valueOf(s)).reduce((integer1, integer2)->(integer1+integer2));
    	
    	/*Version extendida
    	//Paso 3: Leer numeros de fichero. Lee línea a linea y crea una lista con lo leido
    	JavaRDD<String> lines = context.textFile(args[0]);
    	//Paso 4: Transformo el rdd de string en un rdd de enteros haciendo un map.
    	JavaRDD<Integer> rddOfIntegers=lines.map(s->Integer.valueOf(s));
    	//Paso 5: Le paso al reduce una funcion que sume dos enteros
    	int sum=rddOfIntegers.reduce((integer1, integer2)->(integer1+integer2));
    	*/
    	//Paso 6: Se imprime el resultado
        System.out.println( "The sum is: "+sum );
        //Paso 7: Cerramos el contexto
        context.stop();
    }
}
