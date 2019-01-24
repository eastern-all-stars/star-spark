package com.easternallstars.star.spark.job.task.process;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.easternallstars.star.spark.common.Util;
import com.easternallstars.star.spark.job.task.TaskConf;

public class SqlProcess extends Process {

	public SqlProcess(TaskConf conf) {
		super(conf);
	}

	@Override
	public Dataset<Row> execute(SparkSession ss, Map<String, Dataset<Row>> inputMap) {
		inputMap.forEach((name, df) -> {
			logger.info("create temp table: " + name);
			df.createOrReplaceTempView(name);
		});
		
		String sql = Util.getValueFromOption(conf, "sql");
		logger.info("sql:" + sql);
		Dataset<Row> ds = ss.sql(sql);
		
		logger.info("schema" + ds.schema().treeString());
		return ds;
	}
}
