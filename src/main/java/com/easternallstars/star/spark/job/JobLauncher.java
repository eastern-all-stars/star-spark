package com.easternallstars.star.spark.job;

import org.apache.spark.sql.SparkSession;

public class JobLauncher {

	public static void main(String[] args) {
		String configFilePath = parseArguments(args);
		JobDescription jobDesc = new JobDescription(configFilePath);
	
		SparkSession ss = SparkSession
	    	      .builder()
	    	      .appName(jobDesc.getName())
	    	      .config("spark.driver.maxResultSize", "20g")
	    	      .config("spark.jars", "D:\\log4j-1.2-api-2.5.jar")
//	    	      .enableHiveSupport()
	    	      .getOrCreate();

		Job job = new Job(ss, jobDesc);
		job.run();
	}

	private static String parseArguments(String[] args) {
		if (args.length != 1) {
			showUsage();
			throw new IllegalArgumentException("illegal");
		}

		return args[0];
	}

	private static void showUsage() {
		System.out.println("Usage: JobLaucher config.json");
	}
}
