package com.easternallstars.star.spark.job.task.source;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.easternallstars.star.spark.common.Util;
import com.easternallstars.star.spark.job.task.TaskConf;

public class KafkaSource extends Source {
    private StructType avroStructType = new StructType(new StructField[]{
            new StructField("event_timestamp", DataTypes.StringType, true, Metadata.empty()),
            new StructField("level", DataTypes.StringType, true, Metadata.empty()),
            new StructField("logger", DataTypes.StringType, true, Metadata.empty()),
            new StructField("host", DataTypes.StringType, true, Metadata.empty()),
            new StructField("thread", DataTypes.StringType, true, Metadata.empty()),
            new StructField("body", DataTypes.StringType, true, Metadata.empty())
        });
    
	public KafkaSource(TaskConf conf) {
		super(conf);
	}

	/**
	 * Options:
	 *   kafka.bootstrap.servers : host1:port1,host2:port2
	 *   subscribe : topic1,topic2
	 *   subscribePattern : topic.*
	 *   startingOffsets : earliest
	 *   endingOffsets : latest
	 */
	@Override
	public Dataset<Row> toDF(SparkSession ss) {
		String format = Util.getValueFromOption(conf, "format");
		Dataset<Row> df = ss.readStream().format("kafka")
				.option("kafka.bootstrap.servers", Util.getValueFromOption(conf, "kafka.bootstrap.servers"))
				.option("subscribe", Util.getValueFromOption(conf, "subscribe"))
				.option("startingOffsets", Util.getValueFromOption(conf, "startingOffsets"))
				// .options(conf.getOptions())
				.load();
		switch (format) {
		case "json": // json格式数据
			String schema = Util.getValueFromOption(conf, "schema");
			DataType dataType = DataType.fromJson(schema);
			
			Column valueColunm = new Column("value");
			df = df.selectExpr("CAST(value AS STRING)")
					.select(functions.from_json(valueColunm, dataType)
					.as("struct"))
					.select("struct.*");

			break;
		case "avro": // flume - avro格式数据
			ss.udf().register("deserialize", (byte[] data) -> {
				ByteArrayInputStream in = new ByteArrayInputStream(data);
				BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);

				SpecificDatumReader<AvroFlumeEvent> reader = new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class);
				AvroFlumeEvent event = reader.read(null, decoder);
				
				Map<String, String> stringMap = new HashMap<String, String>();
				for (Map.Entry<CharSequence, CharSequence> entry : event.getHeaders().entrySet()) {
					stringMap.put(entry.getKey().toString(), entry.getValue().toString());
				}

				String body = new String(event.getBody().array(), "UTF-8");
				Event e = EventBuilder.withBody(body, Charset.forName("UTF-8"), stringMap);
				Map<String, String> headers = e.getHeaders();
	            return RowFactory.create(
	            		headers.get("timestamp"),
	            		headers.get("level"),
	            		headers.get("logger"),
	            		headers.get("host"),
	            		headers.get("thread"),
	            		body);
			}, avroStructType);

			df = df
	                .select("value")
	                .as(Encoders.BINARY())
	                .selectExpr("deserialize(value) as rows")
	                .select("rows.*");
			
			break;
		}
		
		return df;
	}

}
