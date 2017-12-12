package com.test.spark.rdd;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleRDD {

	public static void main(String[] args) {
		 SparkConf config = new SparkConf();
		 
		 JavaSparkContext sc = new JavaSparkContext("local[*]", "SimpleJob", config);
		 
		 JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
		 
		 System.out.println("Count : " + rdd.count() );
		 
		 sc.stop();
	}

}
