package twitteranalytics.kafkaproducer;

import io.github.redouane59.twitter.TwitterClient;
import io.github.redouane59.twitter.signature.TwitterCredentials;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import twitteranalytics.kafkaproducer.configs.KafkaConfig;
import twitteranalytics.kafkaproducer.configs.TwitterConfig;


import com.github.scribejava.core.model.Response;import com.google.gson.Gson;


public class TwitterProducer {
	private KafkaProducer<String, String> producer;
	private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(30);
	public static void main(String[] args) { new TwitterProducer().run(args[1], args[0]); }
	
	public TwitterClient createTwitterClient(){
		TwitterClient client = new TwitterClient(TwitterCredentials.builder()
				.accessToken(TwitterConfig.TOKEN)
				.accessTokenSecret(TwitterConfig.SECRET)
				.apiKey(TwitterConfig.CONSUMER_KEY)
				.apiSecretKey(TwitterConfig.CONSUMER_SECRET).build());
		return client;
	}
	
	public KafkaProducer<String, String> createKafkaProducer() {
		Properties prop = new Properties();
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAPSERVERS);
		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		return new KafkaProducer<String, String>(prop);
	}
	
	private void run(String searchKey, String language) {
		producer = createKafkaProducer();
		TwitterClient client = createTwitterClient();
		
		client.deleteFilteredStreamRule(searchKey);
		try{
			client.addFilteredStreamRule(searchKey, searchKey);
		} catch (Exception e){
			e.printStackTrace();
		}
		
		final String lang = language.length() == 2 ? language : "en"; // English as default for invalid language
		Future<Response> future = client.startFilteredStream((tweet) -> {
			if(tweet.getLang().equals(lang)){
				TweetDTO tweetObject = new TweetDTO();
				tweetObject.setId(tweet.getId());
				tweetObject.setTweet(tweet.getText());
				tweetObject.setName(tweet.getUser().getName());
				tweetObject.setLocation(tweet.getUser().getLocation());
				producer.send(new ProducerRecord<String, String>(KafkaConfig.TOPIC, null, new Gson().toJson(tweetObject)));
			}
		});
		
//		if(future.isDone()){
//			client.deleteFilteredStreamRule(searchKey);
//		}
	}
}
