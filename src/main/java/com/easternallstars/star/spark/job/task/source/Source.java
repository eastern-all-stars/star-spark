package com.easternallstars.star.spark.job.task.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.easternallstars.star.spark.job.task.Task;
import com.easternallstars.star.spark.job.task.TaskConf;

public abstract class Source extends Task {
	public Source(TaskConf conf) {
		super(conf);
	}

	public abstract Dataset<Row> toDF(SparkSession ss);
}

