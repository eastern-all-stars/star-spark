package com.easternallstars.star.spark.job.task;

import java.util.List;
import java.util.Map;

public class TaskConf {

	private String step;
	
	private String name;
	
	private String type;
	
	private List<String> inputs;
	
	private Map<String, String> options;

	public TaskConf(String step) {
		this.step = step;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<String> getInputs() {
		return inputs;
	}

	public void setInputs(List<String> inputs) {
		this.inputs = inputs;
	}

	public Map<String, String> getOptions() {
		return options;
	}

	public void setOptions(Map<String, String> options) {
		this.options = options;
	}

	public String getStep() {
		return step;
	}
}
