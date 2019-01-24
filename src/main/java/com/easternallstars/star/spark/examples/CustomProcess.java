package com.easternallstars.star.spark.examples;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.easternallstars.star.spark.job.task.TaskConf;
import com.easternallstars.star.spark.job.task.process.Process;

public class CustomProcess extends Process {
	private Dataset<Row> ret = null;

	public CustomProcess(TaskConf conf) {
		super(conf);
	}

	@Override
	public Dataset<Row> execute(SparkSession ss, Map<String, Dataset<Row>> inputMap) {
		inputMap.forEach((name, df) -> {
			ret = df.filter(df.col("name").contains("a"));
		});
		return ret;
	}
}
