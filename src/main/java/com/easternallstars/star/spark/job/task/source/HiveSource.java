package com.easternallstars.star.spark.job.task.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.easternallstars.star.spark.common.Util;
import com.easternallstars.star.spark.job.task.TaskConf;

public class HiveSource extends Source {

	public HiveSource(TaskConf conf) {
		super(conf);
	}

	@Override
	public Dataset<Row> toDF(SparkSession ss) {
		String sql = Util.getValueFromOption(conf, "sql");
		return ss.sql(sql);
	}
}
