package com.easternallstars.star.spark.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.easternallstars.star.spark.job.task.TaskConf;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class Util {
	protected static Log logger = LogFactory.getLog(Util.class);
	
	public static boolean isEmpty(String str) {
		return str == null || str.length() == 0;
	}
	
	public static int getIntValueFromOption(TaskConf conf, String key) {
		String value = getValueFromOption(conf, key, true);
		return Integer.parseInt(value);
	}
	
	public static long getLongValueFromOption(TaskConf conf, String key) {
		String value = getValueFromOption(conf, key, true);
		return Long.parseLong(value);
	}
	
	public static String getValueFromOption(TaskConf conf, String key) {
		return getValueFromOption(conf, key, true);
	}
	
	public static int[] getIntArrayFromOption(TaskConf conf, String key) {
		String value = getValueFromOption(conf, key, true);
		String[] stringArray = value.split(",");
		int[] ret = new int[stringArray.length];
		for (int i = 0; i < stringArray.length; i++) {
			ret[i] = Integer.valueOf(stringArray[i]);
		}
		
		return ret;
	}
	
	/**
	 * @param conf
	 * @param key
	 * @param isRequired 是否必填项
	 * @return
	 */
	public static  String getValueFromOption(TaskConf conf, String key, boolean isRequired) {
		String value = conf.getOptions().get(key);
		if (isRequired && Util.isEmpty(value)) {
			throw new IllegalArgumentException(String.format("you should give a %s for %s %s", key, conf.getType(), conf.getStep())); 
		}
		return value;
	}

	public static void extractConf(TaskConf conf, JsonObject obj) {
		conf.setType(obj.get("type").getAsString());
		conf.setName(obj.get("name").getAsString());
		
		List<String> inputs = new ArrayList<>();
		try {
			JsonArray array = obj.getAsJsonArray("inputs");
			if (array != null && array.size() > 0) { 
				array.forEach(json -> inputs.add(json.getAsString()));
			}
		} catch (Exception e) {
			logger.warn("Failed to parse inputs", e);
		}
		conf.setInputs(inputs);
		
		Map<String, String> options = new HashMap<>();
		try {
			JsonObject jsonObject = obj.getAsJsonObject("options");
			if (jsonObject != null) {
				jsonObject.entrySet().forEach(entry -> options.put(entry.getKey(), entry.getValue().getAsString()));
			}
		} catch (Exception e) {
			logger.warn("Failed to parse options", e);
		}
		conf.setOptions(options);
	}
}
