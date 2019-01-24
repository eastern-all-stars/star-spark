package com.easternallstars.star.spark.job.task.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.easternallstars.star.spark.common.Util;
import com.easternallstars.star.spark.job.task.TaskConf;

public class PhoenixSource extends Source {

	public PhoenixSource(TaskConf conf) {
		super(conf);
	}

	@Override
	public Dataset<Row> toDF(SparkSession ss) {
		String sql = Util.getValueFromOption(conf, "sql");
		
		String wrappedSql = String.format("(%s) as tmp", sql);
		Dataset<Row> ds = ss.read()
				  .format("jdbc")
				  .option("driver", Util.getValueFromOption(conf, "driver"))
				  .option("url", Util.getValueFromOption(conf, "url"))
				  .option("dbtable", wrappedSql)
				  .option("user", Util.getValueFromOption(conf, "user"))
				  .option("password", Util.getValueFromOption(conf, "password"))
				  .load();
		
		return ds;
	}

}
