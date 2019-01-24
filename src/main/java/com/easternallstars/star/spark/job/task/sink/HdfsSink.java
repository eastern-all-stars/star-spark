package com.easternallstars.star.spark.job.task.sink;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;

import com.easternallstars.star.spark.common.Util;
import com.easternallstars.star.spark.job.task.TaskConf;

public class HdfsSink extends Sink {
	protected String format;

	public HdfsSink(TaskConf conf) {
		super(conf);
		this.format = Util.getValueFromOption(conf, "format");
	}

	public Dataset<Row> preprocess(Dataset<Row> df) {
		return df;
	}
	
	public void write(Dataset<Row> inputDf) {
		Dataset<Row> df = repartition(inputDf, inputDf.sparkSession().sparkContext().defaultParallelism());
		if (inputDf.isStreaming()) {
			writeStream(df.writeStream());
		} else {
			writeBatch(df.write());
		}
	}
	
	protected void writeStream(DataStreamWriter<Row> writer) {
		writer.queryName(conf.getName())
			.outputMode(Util.getValueFromOption(conf, "outputMode"))
			.format(format)
			.trigger(Trigger.ProcessingTime(Util.getLongValueFromOption(conf, "interval"))) // 间隔
			.option("path", Util.getValueFromOption(conf, "path"))
			.option("checkpointLocation", "/tmp/streamingjob/" + conf.getName())
			//.options(conf.getOptions())
			.start();
	}

	protected void writeBatch(DataFrameWriter<Row> writer) {
		writeBatchInner(writer.format(format).mode(getSaveModeFromOption()));
	}
	
	private SaveMode getSaveModeFromOption() {
		String valueFromOption = Util.getValueFromOption(conf, "saveMode", false);
		SaveMode defaultMode = SaveMode.Overwrite;
		if (Util.isEmpty(valueFromOption)) {
			return defaultMode;
		}
		
		SaveMode ret = null;
		switch (valueFromOption) {
		case "append":
			ret = SaveMode.Append;
			break;
		case "errorIfExists":
			ret = SaveMode.ErrorIfExists;
			break;
		case "ignore":
			ret = SaveMode.Ignore;
			break;
		default:
			ret = defaultMode;
			break;
		}

		return ret;
	}

	private void writeBatchInner(DataFrameWriter<Row> writer) {
		String outputPath = Util.getValueFromOption(conf, "path");
		writer.save(outputPath);
	}

	protected Dataset<Row> repartition(Dataset<Row> df, int defaultParallelism) {
		return df;
	}
}
