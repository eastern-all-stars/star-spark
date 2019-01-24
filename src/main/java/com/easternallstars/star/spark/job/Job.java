package com.easternallstars.star.spark.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.easternallstars.star.spark.job.task.process.Process;
import com.easternallstars.star.spark.model.Pair;

public class Job {
	private SparkSession ss;
	private JobDescription jobDesc;
	
	private Map<String, Dataset<Row>> dfMap = new HashMap<>();

	public Job(SparkSession ss, JobDescription jobDesc) {
		this.ss = ss;
		this.jobDesc = jobDesc;
	}

	public void run() {
		// source 
		jobDesc.getSources().forEach(source -> {
			dfMap.put(source.getConf().getName(), source.toDF(ss));
		});
		
		// process
		List<Pair<String, Dataset<Row>>> processRst = null;
		// do {
			processRst = getValidProcess(jobDesc.getProcesses());
			processRst.forEach(pair -> dfMap.put(pair.fst, pair.snd));
		// } while (!processRst.isEmpty());
		
		// sink
		jobDesc.getSinks().forEach(s -> {
//			s.getConf().getInputs().stream().map(input -> dfMap.get(input)).flat
//			s.getConf().getInputs().stream().flatMap(mapper).collect(Collectors.toList());
//			Dataset<Row> inputDFs = s.getConf().getInputs().stream().map(input -> dfMap.get(input)).flatMap(Arrays::stream).collect(Collectors.toList());
//			
//			// use only first input
			
			Dataset<Row> inputDf = dfMap.get(s.getConf().getInputs().get(0)); // 只选择第一个input
			s.write(inputDf);
		});
		
		// if stream query exist
		if (ss.streams().active().length > 0) {
			try {
				ss.streams().awaitAnyTermination();
			} catch (StreamingQueryException e) {
				throw new RuntimeException("await failed.", e);
			}
		}
	}

	private List<Pair<String, Dataset<Row>>> getValidProcess(List<Process> processes) {
		// Set<String> dfKeys = dfMap.keySet();
		
		/*processes.stream().filter(p -> {
			boolean existAllInput = true;
			p.getConf().getInputs().forEach(input -> existAllInput = dfKeys.contains(input));
			return dfKeys.contains(p.getConf().getName()) && existAllInput;
		});*/
		List<Pair<String, Dataset<Row>>> validProcesses = new ArrayList<>();
		processes.forEach(p -> validProcesses.add(new Pair<String, Dataset<Row>>(p.getConf().getName(), p.execute(ss, dfMap))));
		return validProcesses;
	}
}
