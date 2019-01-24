package com.easternallstars.star.spark.model;

import java.io.Serializable;

import org.apache.spark.sql.types.DataType;

import com.easternallstars.star.spark.common.HBaseUtil;

public class HBaseRowKey implements Serializable {
	private static final long serialVersionUID = 2027281740650306465L;
	private String columnName;
	private String type;
	private DataType dataType;
	private int index;
	
	public HBaseRowKey(String[] params) {
		columnName = params[0];
		type = params[1];
		if (params.length == 3) {
			index = Integer.parseInt(params[2]);
		}
		dataType = HBaseUtil.convertToDataType(type);
	}

	public String getColumnName() {
		return columnName;
	}

	public String getType() {
		return type;
	}

	public DataType getDataType() {
		return dataType;
	}

	public int getIndex() {
		return index;
	}
}
