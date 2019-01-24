package com.easternallstars.star.spark.job.task.sink;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.easternallstars.star.spark.common.HBaseUtil;
import com.easternallstars.star.spark.common.Util;
import com.easternallstars.star.spark.job.task.TaskConf;
import com.easternallstars.star.spark.model.HBaseColumn;
import com.easternallstars.star.spark.model.HBaseRowKey;

public class HBaseSink extends Sink {

	public HBaseSink(TaskConf conf) {
		super(conf);
	}

	@Override
	public Dataset<Row> preprocess(Dataset<Row> df) {
		return df;
	}

	@Override
	public void write(Dataset<Row> df) {
		HBaseRowKey rowkey = HBaseUtil.extractRowKeyConfig(conf);
		List<HBaseColumn> columns = HBaseUtil.extractColsConfig(conf);
		
		if (df.isStreaming()) {
			throw new RuntimeException("Unsupported data stream");
			// writeStream(df.writeStream());
		} else {
	        Configuration configuration = HBaseConfiguration.create();
	        JobConf jobConf = new JobConf(configuration);
	        jobConf.setOutputFormat(TableOutputFormat.class);
	        jobConf.set(TableOutputFormat.OUTPUT_TABLE, Util.getValueFromOption(conf, "tableName"));
	        
			JavaPairRDD<ImmutableBytesWritable, Put> hbasePutsPair = df.toJavaRDD().mapToPair(row -> HBaseUtil.generatePut(row, rowkey, columns));
			// hbasePutsPair.saveAsNewAPIHadoopDataset(jobConf);
			hbasePutsPair.saveAsHadoopDataset(jobConf);
		}
	}
}
