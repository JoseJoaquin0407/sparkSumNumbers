package org.jj.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

public class SumNumbers 
{
    public static void main( String[] args )
    {
    	//Paso 1: Acceder a Spark y configurarlo
    	SparkConf conf= new SparkConf();
    	//Paso 2: Creamos el contexto a partir de la configuración 
    	JavaSparkContext context=new JavaSparkContext(conf);
    	//Paso 3: Definimos un array de enteros
    	Integer[] numbers = new Integer[]{1,2,3,4,5,6,7,8,9};
    	//Paso 4: Creamos una lista de números
    	List<Integer> listOfNumbers=Arrays.asList(numbers);
    	//Paso 5: Crea una lista especial de Spark
    	JavaRDD<Integer> rddOfNumbers = context.parallelize(listOfNumbers);
    	//Paso 6: Al reduce se le pasa una funcion que sume dos enteros
    	int sum=rddOfNumbers.reduce((integer1, integer2)->(integer1+integer2));
    	//Paso 7: Se imprime el resultado
        System.out.println( "The sum is: "+sum );
        //Paso 8: Cerramos el contexto
        context.stop();
    }
}
