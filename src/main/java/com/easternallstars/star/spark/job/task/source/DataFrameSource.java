package com.easternallstars.star.spark.job.task.source;

import com.easternallstars.star.spark.common.DataFrameUtil;
import com.easternallstars.star.spark.common.HBaseUtil;
import com.easternallstars.star.spark.job.task.TaskConf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import java.util.Arrays;
import java.util.List;

public class DataFrameSource extends Source {
	public DataFrameSource(TaskConf conf) {
		super(conf);
	}
	@Override
	public Dataset<Row> toDF(SparkSession ss) {
		List<String> columns = DataFrameUtil.extractColsConfig(conf);
		Object[] values = new Object[columns.size()];
		StructField[] structField = new StructField[columns.size()];
		for (int i = 0; i < values.length; i++) {
			String[] subStrArray = columns.get(i).split(":");
			DataType dataType = HBaseUtil.convertToDataType(subStrArray[2]);
			values[i] = subStrArray[1];
			structField[i]= new StructField(subStrArray[0], dataType, false, Metadata.empty());
		}
		List<Row> data = Arrays.asList(RowFactory.create(values));
		StructType schema = new StructType(structField);
		Dataset<Row> df = ss.createDataFrame(data, schema);
		return df;
	}
}
