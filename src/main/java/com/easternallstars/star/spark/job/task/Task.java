package com.easternallstars.star.spark.job.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Task {
	protected Log logger = LogFactory.getLog(this.getClass());
	
	protected TaskConf conf;
	
	public Task() {
	}
	
	public Task(TaskConf conf) {
		this.conf = conf;
	}

	public TaskConf getConf() {
		return conf;
	}

	public void setConf(TaskConf conf) {
		this.conf = conf;
	}
}
