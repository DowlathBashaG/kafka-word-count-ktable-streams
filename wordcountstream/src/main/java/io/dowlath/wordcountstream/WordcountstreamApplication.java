package io.dowlath.wordcountstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class WordcountstreamApplication {

	public static void main(String[] args) {
		SpringApplication.run(WordcountstreamApplication.class, args);
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG,"streams-wordcount");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,0);

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String,String> stream = builder.stream("streams-wordcount-input");
		KGroupedStream<String,String> kGroupedStream = stream.flatMapValues(value-> Arrays.asList(value.toLowerCase().split(" ")))
						.groupBy((key,value) -> value);
		KTable<String,Long> countsTable = kGroupedStream.count();
		countsTable.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(),Serdes.Long()));


		stream.foreach((key,value) -> System.out.println("Key and Value "+ key+ " " + value));
		stream.filter((key,value) -> value.contains("token"))
				.mapValues(value -> value.toUpperCase())
				.to("streams-dataflow-output");
		Topology topology = builder.build();
		System.out.println(topology.describe());

		KafkaStreams streams = new KafkaStreams(topology,props);
		//streams.start();

		//Streams never closed. we need to access and close
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));




	}

}
