package com.easternallstars.star.spark.job.task.process;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.easternallstars.star.spark.job.task.Task;
import com.easternallstars.star.spark.job.task.TaskConf;

public abstract class Process extends Task {
	public Process() {
	}

	public Process(TaskConf conf) {
		super(conf);
	}

	public abstract Dataset<Row> execute(SparkSession ss, Map<String, Dataset<Row>> inputMap);

}
