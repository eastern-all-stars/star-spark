package com.easternallstars.star.spark.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.easternallstars.star.spark.job.task.TaskConf;
import com.easternallstars.star.spark.model.HBaseColumn;
import com.easternallstars.star.spark.model.HBaseRowKey;

import scala.Tuple2;

public class HBaseUtil {
	protected     static        Log    logger     = LogFactory.getLog(HBaseUtil.class);
	private static final String ROWKEY_KEY = "rowkey";
	private static final String COLS_KEY   = "cols";

	public static String convertScanToString(Scan scan) {
        ClientProtos.Scan proto;
		try {
			proto = ProtobufUtil.toScan(scan);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return Base64.encodeBytes(proto.toByteArray());
	}
	
	public static DataType convertToDataType(String type) {
		switch(type) {
		case "int":
			return DataTypes.IntegerType;
		case "long":
		case "bigint":
			return DataTypes.LongType;
		default:
			return DataTypes.StringType;
		}
	}
	
	public static Object bytesToObject(String type, byte[] bytes) {
		try {
			switch(type) {
				case "int":
					return Bytes.toInt(bytes);
				case "long":
				case "bigint":
					return Bytes.toLong(bytes);
				default:
					return Bytes.toString(bytes);
			}
		} catch (Exception e){
			logger.info(String.format("bytesToObject error:type=%s, bytes=%s", type, Bytes.toString(bytes)), e);
			return null;
		}

	}
	
	public static byte[] readDataFromRow(String type, int index, Row row) {
		if (row.isNullAt(index)) {
			return null;
		}
		switch(type) {
		case "int":
				return Bytes.toBytes(row.getInt(index));

		case "long":
		case "bigint":
			return Bytes.toBytes(row.getLong(index));
		default:
			return Bytes.toBytes(row.getString(index));
		}
	}
	
	public static List<HBaseColumn> extractColsConfig(TaskConf conf) {
		String cols = Util.getValueFromOption(conf, COLS_KEY, false);
		if (Util.isEmpty(cols)) {
			return null;
		}
		
		String[] stringArray = cols.split(",");
		List<HBaseColumn> columns = new ArrayList<>();
		for (String str : stringArray) {
			String[] subStrArray = str.split(":");
			columns.add(new HBaseColumn(subStrArray));
		}

		return columns;
	}
	
	public static HBaseRowKey extractRowKeyConfig(TaskConf conf) {
		String cols = Util.getValueFromOption(conf, ROWKEY_KEY);
		String[] params = cols.split(":");
		return new HBaseRowKey(params);
	}
	
	public static StructType generateStructType(HBaseRowKey rowkey, List<HBaseColumn> columns) {
		List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField(rowkey.getColumnName(), rowkey.getDataType(), false));
        
        if (columns != null && columns.size() > 0) { 
	        for (HBaseColumn col : columns) {
	        	structFields.add(DataTypes.createStructField(col.getColumnName(), col.getDataType(), true));
			}
        }

        StructType schema = DataTypes.createStructType(structFields);
        return schema;
	}
	
	public static Row generateRow(Result result, HBaseRowKey rowkey, List<HBaseColumn> columns) {
		int colSize = (columns != null && columns.size() > 0) ? columns.size() : 0;
		Object[] objs = new Object[colSize + 1];
		objs[0] = bytesToObject(rowkey.getType(), result.getRow());
		for (int i = 0; i < colSize; i++) {
			HBaseColumn col = columns.get(i);
			objs[i + 1] = bytesToObject(col.getType(), result.getValue(Bytes.toBytes(col.getColFamily()), Bytes.toBytes(col.getColumnName())));
		}
        return RowFactory.create(objs);
	}

	public static Tuple2<ImmutableBytesWritable,Put> generatePut(Row row, HBaseRowKey rowkey, List<HBaseColumn> columns) {
		Put put = new Put(readDataFromRow(rowkey.getType(), rowkey.getIndex(), row));
		
		if (columns != null && columns.size() > 0) { 
			for (HBaseColumn column : columns) {
				put.addColumn(Bytes.toBytes(column.getColFamily()), Bytes.toBytes(column.getColumnName()),
						readDataFromRow(column.getType(), column.getIndex(), row));
			}
		}
        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
	}
}
