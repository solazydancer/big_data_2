package ru.mephi.skok;

import java.io.File;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class CalculatingTest {
	
	/*
	 * Creates new Spark session and asserts if data.csv and result.csv are equal
	 */
	
	@Test
	public void calculatingTest() {
	    	
    	SparkConf conf = new SparkConf()
        .setAppName("Spark")
        .setMaster("local");

    	SparkSession spark = SparkSession
    			.builder()
    			.config(conf)
    			.getOrCreate();
		
    	/*
    	 * result.csv is expected file after job for data.csv
    	 * Comparison is made in strings translated from rows
    	 */
    	
		Dataset<Row> resultDataSet = spark.read().csv("/home/cloudera/workspace/spark/src/test/resources/result.csv");
		
		// result of test job
		String pathToSyslogsFile = "/home/cloudera/workspace/spark/src/test/resources/data.csv";
		Dataset<Row> syslogDataSet = Main.calculate(spark, pathToSyslogsFile);
		
		// check if amounts of rows are equal
		assertEquals(resultDataSet.count(), syslogDataSet.count());
		List<Row> resultList = resultDataSet.collectAsList();
		List<Row> syslogList = syslogDataSet.collectAsList();
		boolean flag = true;
		// comparison of strings
		for (int i = 0; i < resultDataSet.count(); i++) {
			if (!(resultList.get(i).toString()).equals(syslogList.get(i).toString())) flag = false;
		}
		assertEquals(true, flag);
	}
}
