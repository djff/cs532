package twitteranalytics.kafkaproducer;


public class TweetDTO {

	private String id;
	private String tweet;
	private String name;
	private String location;
	
	public void setTweet(String tweet){
		this.tweet = tweet;
	}
	
	public void setName(String name){
		this.name = name;
	}
	
	public void setLocation(String location){
		this.location = location;
	}
	
	public void setId(String id){
		this.id = id;
	}
	
	public String getTweet(){
		return this.tweet;
	}
	
	public String getName(){
		return this.name;
	}
	
	public String getLocation(){
		return this.location;
	}
	
	public String getTweetId(){
		return this.id;
	}
}
