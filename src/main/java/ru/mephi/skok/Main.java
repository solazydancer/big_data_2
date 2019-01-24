package ru.mephi.skok;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

/*
 * Main class calls data generator, reads data from HDFS and calculates how often priority occurs
 * You can view result in console or in given directory
 * @params args[0] -> directory where result of job must be saved
 */

public class Main {

    public static void main(String[] args) {
    	
    	SparkConf conf = new SparkConf()
        .setAppName("Spark")
        .setMaster("local");

    	SparkSession spark = SparkSession
    			.builder()
    			.config(conf)
    			.getOrCreate();

    	Configuration hadoopConf = new Configuration();
    	
    	hadoopConf.set("fs.hdfs.impl", 
    	        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
    	    );
    	hadoopConf.set("fs.file.impl",
    	        org.apache.hadoop.fs.LocalFileSystem.class.getName()
    	    );
    	
    	// Bash script generates data
    	InputDataGenerator generator = new InputDataGenerator();
    	generator.generateBashInput();
    	
    	String pathToDirectory = args[0];
    	
    	String pathInHdfs = "hdfs://quickstart.cloudera:8020/user/spark/data.csv";
    	String pathToResult = readFromHdfs(hadoopConf, pathInHdfs, pathToDirectory);
    	System.out.println(pathToResult);
    	
    	Dataset<Row> syslogDataSet = calculate(spark, pathToResult);
        syslogDataSet.coalesce(1).write().format("com.databricks.spark.csv").save(pathToDirectory + "/finalResult.csv");
    	syslogDataSet.show();
    }
    
    /*
     * Method gets path to file in HDFS and writes data to ordinary CSV file
     * @params HadoopConfiguraton, path to CSV file in HDFS
     * @return path to CSV file with data from HDFS
     */
    
    private static String readFromHdfs(Configuration hadoopConf, String pathToHdfsFile, String pathToResult) {

        String pathForCsvFile = pathToResult + "/result.csv";
        
        try {
        	
            Path path = new Path(pathToHdfsFile);
            FileSystem fs = path.getFileSystem(hadoopConf);
            FSDataInputStream inputStream = fs.open(path);
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader reader = new BufferedReader(inputStreamReader);
            File file = new File(pathForCsvFile);
            file.createNewFile();
            OutputStream outputStream = new FileOutputStream(file);
            IOUtils.copy(reader, outputStream);
            reader.close();
            outputStream.close();
            fs.close();
            byte[] encoded = Files.readAllBytes(Paths.get(pathForCsvFile));
            String string = new String(encoded);
            file.delete();
            BufferedWriter writer = new BufferedWriter(new FileWriter(pathForCsvFile));
            writer.write(string);
            writer.close();
            
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot process file path", e);
        }
        return pathForCsvFile;
    }
    
    /*
     * Method gets CSV file and counts how often priority occurs
     * @params path to file in HDFS (string) and Spark session
     * @return Dataset in format: priority and count
     */

    public static Dataset<Row> calculate(SparkSession spark, String pathToResult) {
    	
        Dataset<Row> syslogDataSet = spark.read().csv(pathToResult);
        syslogDataSet = syslogDataSet.orderBy("_c1");
        return syslogDataSet.groupBy("_c1").count();

    }
}

