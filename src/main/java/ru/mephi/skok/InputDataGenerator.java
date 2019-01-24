package ru.mephi.skok;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

/* Class that runs bash scripts to:
 * 1. generate data in format: hour (current), priority (from 0 to 7) and status to priority (string message) -> data.csv
 * 2. move data.csv to HDFS -> hdfs://quickstart.cloudera:8020/user/spark/data.csv 
 */

public class InputDataGenerator {

    static BufferedReader in;
    String[] start = new String[]{"/bin/sh", "/home/cloudera/workspace/spark/src/main/resources/start.sh"};
    String[] move = new String[]{"/bin/sh", "/home/cloudera/workspace/spark/src/main/resources/move.sh"};
    
    public void generateBashInput() {
    	bashStep(start);
    	bashStep(move);
    	
    }

    public void bashStep(String[] cmd) {
    	try {
    		Process pr = Runtime.getRuntime().exec(cmd);
    		in = new BufferedReader(new InputStreamReader(pr.getErrorStream()));
    		String line = in.readLine();
    		while (line != null) {
    			System.out.println(line);
    			line = in.readLine();
    		}
    	} catch (IOException e) {
            e.printStackTrace();	
    	}
    }
}
