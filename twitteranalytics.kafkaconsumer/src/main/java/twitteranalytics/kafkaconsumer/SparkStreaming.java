package twitteranalytics.kafkaconsumer;


import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.serializer.StringDecoder;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.gson.Gson;

import scala.Tuple2;


public class SparkStreaming {
	private static JavaSparkContext jsc;
	
	@SuppressWarnings({ "deprecation", "serial" })
	public static void main(String[] args) throws IOException {
		System.out.println("Spark Streaming Application Starting ..." + StringDeserializer.class.toString());
		
		SparkStreaming app = new SparkStreaming();
		
		
		Map<String, String> kafkaParams = app.getKafkaParams();
		JavaStreamingContext streamingContext = app.createStreamingContext();
		
		HbaseUtil hbaseUtil = new HbaseUtil(jsc, "local[3]");
		
		JavaPairInputDStream<String, String> stream = 
				KafkaUtils.createDirectStream(
						streamingContext,
						String.class, String.class, 
						StringDecoder.class, 
						StringDecoder.class, 
						kafkaParams,
						Collections.singleton(KafkaConfig.TOPIC));
		stream.filter(new Function<Tuple2<String, String>, Boolean>(){
			@Override
			public Boolean call(Tuple2<String, String> r) throws Exception {
				// TODO Auto-generated method stub
				TweetDTO tweet = new Gson().fromJson(r._2,  TweetDTO.class);
				return tweet.getLocation().equals("usa");
			}
			
		});
		stream.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
			@Override
			public Void call(JavaPairRDD<String, String> tweetPairRdd) throws Exception {
				System.out.println("rds!" + tweetPairRdd.count());
				hbaseUtil.writeNewRow(tweetPairRdd);
				return null;
			}
		});
		
		streamingContext.start();
		streamingContext.awaitTermination();
	}
	
	public JavaStreamingContext createStreamingContext(){
		SparkConf sparkConf = new SparkConf();
		sparkConf
			.setAppName("TwitterStreamConsumer")
			.setMaster("local[*]")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.registerKryoClasses(new Class[]{org.apache.hadoop.hbase.io.ImmutableBytesWritable.class});
		
		jsc = new JavaSparkContext(sparkConf);
		
		return new JavaStreamingContext(jsc, Durations.seconds(1));
	}
	
	public Map<String, String> getKafkaParams(){
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAPSERVERS);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  "StringDeserializer");
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,  "group1");
		
		return kafkaParams;
	}

}