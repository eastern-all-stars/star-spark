package com.easternallstars.star.spark.model;

import java.io.Serializable;

import org.apache.spark.sql.types.DataType;

import com.easternallstars.star.spark.common.HBaseUtil;

public class HBaseColumn implements Serializable {
	private static final long serialVersionUID = -7660777387959097788L;
	private String colFamily;
	private String columnName;
	private String type;
	private DataType dataType;
	private int index;
	
	public HBaseColumn(String[] params) {
		colFamily = params[0];
		columnName = params[1];
		type = params[2];
		if (params.length == 4) {
			index = Integer.parseInt(params[3]);
		}
		dataType = HBaseUtil.convertToDataType(type);
	}

	public String getColFamily() {
		return colFamily;
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
