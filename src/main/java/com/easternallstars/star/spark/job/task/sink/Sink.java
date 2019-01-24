package com.easternallstars.star.spark.job.task.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.easternallstars.star.spark.job.task.Task;
import com.easternallstars.star.spark.job.task.TaskConf;

public abstract class Sink extends Task {
	public Sink(TaskConf conf) {
		super(conf);
	}
	
	public abstract Dataset<Row> preprocess(Dataset<Row> df);
	
	public abstract void write(Dataset<Row> inputDf);
}
