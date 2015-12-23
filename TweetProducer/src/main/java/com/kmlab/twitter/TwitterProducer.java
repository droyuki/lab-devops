package com.kmlab.twitter;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TwitterProducer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    /**
     * Information necessary for accessing the Twitter API
     */
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private String keywords;
    private String[] filterKeywords;

    /**
     * The actual Twitter stream. It's set up to collect raw JSON data
     */
    private TwitterStream twitterStream;

    private void start(Context context) {
        /** Producer properties **/
        Properties props = new Properties();
        props.put("metadata.broker.list", context.getString(TwitterSourceConstant.BROKER_LIST));
        props.put("serializer.class", context.getString(TwitterSourceConstant.SERIALIZER));
        props.put("request.required.acks", context.getString(TwitterSourceConstant.REQUIRED_ACKS));

        ProducerConfig config = new ProducerConfig(props);

        final Producer<String, String> producer = new Producer<String, String>(config);

        /** Twitter properties **/
        consumerKey = context.getString(TwitterSourceConstant.CONSUMER_KEY_KEY);
        consumerSecret = context.getString(TwitterSourceConstant.CONSUMER_SECRET_KEY);
        accessToken = context.getString(TwitterSourceConstant.ACCESS_TOKEN_KEY);
        accessTokenSecret = context.getString(TwitterSourceConstant.ACCESS_TOKEN_SECRET_KEY);
        keywords = context.getString(TwitterSourceConstant.KEYWORDS_KEY);
        filterKeywords = keywords.split(" ");

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        final Map<String, String> headers = new HashMap<String, String>();

        /** Twitter listener **/
        StatusListener listener = new StatusListener() {
            // The onStatus method is executed every time a new tweet comes in
            public void onStatus(Status status) {
                // The EventBuilder is used to build an event using the
                // the raw JSON of a tweet
                //logger.info(status.getUser().getScreenName() + ": " + status.getText());
                String msg = "";
                if (!status.getLang().equalsIgnoreCase("zh-tw")) {
                    msg = status.getText();
                }
                //KeyedMessage<String, String> data = new KeyedMessage<String, String>("twitter.live", DataObjectFactory.getRawJSON(status));
                KeyedMessage<String, String> data = new KeyedMessage<String, String>("twitter.live", msg);
                producer.send(data);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onScrubGeo(long userId, long upToStatusId) {
            }

            public void onException(Exception ex) {
                logger.info("Shutting down Twitter sample stream...");
                twitterStream.shutdown();
            }

            public void onStallWarning(StallWarning warning) {
            }
        };

        /** Add A filter for the keywords **/
        FilterQuery fq = new FilterQuery();
        fq.track(filterKeywords);

        /** Bind the listener **/
        twitterStream.addListener(listener);
        twitterStream.filter(fq);

        /** twitterStream.sample();   **/
    }

    public static void main(String[] args) {
        try {
            Context context = new Context(args[0]);
            TwitterProducer tp = new TwitterProducer();
            tp.start(context);
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }
}