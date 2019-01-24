package com.easternallstars.star.spark.job.task.source;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.easternallstars.star.spark.common.HBaseUtil;
import com.easternallstars.star.spark.common.Util;
import com.easternallstars.star.spark.job.task.TaskConf;
import com.easternallstars.star.spark.model.HBaseColumn;
import com.easternallstars.star.spark.model.HBaseRowKey;

import scala.Tuple2;

public class HBaseSource extends Source {
	public HBaseSource(TaskConf conf) {
		super(conf);
	}

	@Override
	public Dataset<Row> toDF(SparkSession ss) {
		HBaseRowKey rowkey = HBaseUtil.extractRowKeyConfig(conf);
		List<HBaseColumn> columns = HBaseUtil.extractColsConfig(conf);
		
        Scan scan = new Scan();
        // TODO support filter for scan
        
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(TableInputFormat.INPUT_TABLE, Util.getValueFromOption(conf, "tableName"));
        configuration.set(TableInputFormat.SCAN, HBaseUtil.convertScanToString(scan));

		RDD<Tuple2<ImmutableBytesWritable, Result>> rawRdd = ss.sparkContext().newAPIHadoopRDD(configuration,
				TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		JavaRDD<Row> hbaseRDD = rawRdd.toJavaRDD().map(tuple -> HBaseUtil.generateRow(tuple._2(), rowkey, columns));

		Dataset<Row> df = ss.createDataFrame(hbaseRDD, HBaseUtil.generateStructType(rowkey, columns));
		return df;
	}
}
