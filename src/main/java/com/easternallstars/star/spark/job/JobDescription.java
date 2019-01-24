package com.easternallstars.star.spark.job;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.easternallstars.star.spark.common.Util;
import com.easternallstars.star.spark.job.task.TaskConf;
import com.easternallstars.star.spark.job.task.process.Process;
import com.easternallstars.star.spark.job.task.process.SqlProcess;
import com.easternallstars.star.spark.job.task.sink.HBaseSink;
import com.easternallstars.star.spark.job.task.sink.HdfsSink;
import com.easternallstars.star.spark.job.task.sink.Sink;
import com.easternallstars.star.spark.job.task.source.DataFrameSource;
import com.easternallstars.star.spark.job.task.source.HBaseSource;
import com.easternallstars.star.spark.job.task.source.HiveSource;
import com.easternallstars.star.spark.job.task.source.KafkaSource;
import com.easternallstars.star.spark.job.task.source.PhoenixSource;
import com.easternallstars.star.spark.job.task.source.Source;
import com.google.gson.Gson;
import com.google.gson.JsonObject;


public class JobDescription {
	protected Log logger = LogFactory.getLog(this.getClass());

	private String name;
	private List<Source> sources = new ArrayList<>();
	private List<Process> processes = new ArrayList<>();
	private List<Sink> sinks = new ArrayList<>();

	public JobDescription(String configFilePath) {
		String fileContent;
		try {
			fileContent = new String(Files.readAllBytes(Paths.get(configFilePath)), StandardCharsets.UTF_8);
			logger.info("fileContent:" + fileContent);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read config file", e);
		}
		
		JsonObject obj = new Gson().fromJson(fileContent, JsonObject.class);
		name = obj.get("name").getAsString();
		
		obj.get("source").getAsJsonArray().forEach(json -> getSource(json.getAsJsonObject()));
		obj.get("process").getAsJsonArray().forEach(json -> getProcess(json.getAsJsonObject()));
		obj.get("sink").getAsJsonArray().forEach(json -> getSink(json.getAsJsonObject()));
	}

	private void getSource(JsonObject obj) {
		TaskConf conf = new TaskConf("source");
		Util.extractConf(conf, obj);
		String type = obj.get("type").getAsString();
		Source source = null;
		switch(type) {
		case "hive":
			source = new HiveSource(conf);
			break;
		case "kafka":
			source = new KafkaSource(conf);
			break;
		case "phoenix": // phoenix /hbase
			source = new PhoenixSource(conf);
			break;
		case "hbase":
			source = new HBaseSource(conf);
			break;
		case "dataframe":
			source = new DataFrameSource(conf);
			break;
		default:
			throw new IllegalArgumentException("unsupported source type :" + type);
		}
		
		sources.add(source);
	}
	
	private void getProcess(JsonObject obj) {
		TaskConf conf = new TaskConf("process");
		Util.extractConf(conf, obj);
		String type = obj.get("type").getAsString();
		Process process = null;
		switch(type) {
		case "sql":
			process = new SqlProcess(conf);
			break;
		case "custom":
			String customClass = Util.getValueFromOption(conf, "class");
			try {
				process = (Process) Class.forName(customClass)
					.getConstructor(TaskConf.class)
					.newInstance(conf);
			} catch (Exception e) {
				throw new IllegalArgumentException("process custom class load error: " + customClass, e);
			}
			break;
		default:
			throw new IllegalArgumentException("unsupported process type :" + type);
		}
		
		processes.add(process);
	}
	
	private void getSink(JsonObject obj) {
		TaskConf conf = new TaskConf("sink");
		Util.extractConf(conf, obj);
		String type = obj.get("type").getAsString();
		Sink sink = null;
		switch(type) {
		case "hdfs":
			sink = new HdfsSink(conf);
			break;
		case "hbase":
			sink = new HBaseSink(conf);
			break;
		case "custom":
			String customClass = Util.getValueFromOption(conf, "class");
			try {
				sink = (Sink) Class.forName(customClass)
					.getConstructor(TaskConf.class)
					.newInstance(conf);
			} catch (Exception e) {
				throw new IllegalArgumentException("sink custom class load error: " + customClass, e);
			}
			break;
		default:
			throw new IllegalArgumentException("unsupported sink type :" + type);
		}
		
		sinks.add(sink);
	}
	
	public JobDescription(String name, List<Source> sources, List<Process> processes, List<Sink> sinks) {
		this.name = name;
		this.sources = sources;
		this.processes = processes;
		this.sinks = sinks;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Source> getSources() {
		return sources;
	}

	public void setSources(List<Source> sources) {
		this.sources = sources;
	}

	public List<Process> getProcesses() {
		return processes;
	}

	public void setProcesses(List<Process> processes) {
		this.processes = processes;
	}

	public List<Sink> getSinks() {
		return sinks;
	}

	public void setSinks(List<Sink> sinks) {
		this.sinks = sinks;
	}
}
