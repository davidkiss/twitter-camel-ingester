package com.kaviddiss.twittercamel.elasticsearch;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import twitter4j.Status;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by david on 2015-08-21.
 */
public class ElasticSearchTweetConverter implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {
        Status tweet = exchange.getIn().getBody(Status.class);
        Map map = new HashMap();
        map.put("id", tweet.getId());
        map.put("author", tweet.getUser().getId());
        map.put("content", tweet.getText());
        map.put("created_at", tweet.getCreatedAt().getTime());
        map.put("lang", tweet.getUser().getLang());
        exchange.getIn().setBody(map);
    }
}
