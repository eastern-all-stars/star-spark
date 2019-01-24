package com.easternallstars.star.spark.common;


import java.util.ArrayList;
import java.util.List;

import com.easternallstars.star.spark.job.task.TaskConf;

public class DataFrameUtil {
	private static final String COLS_KEY = "data";
	public static List<String> extractColsConfig(TaskConf conf) {
		String cols = Util.getValueFromOption(conf, COLS_KEY);
		String[] stringArray = cols.split(",");
		List<String> columns = new ArrayList<>();
		for (String str : stringArray) {
			columns.add(str);
		}
		return columns;
	}
}
