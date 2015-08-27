package com.kaviddiss.twittercamel.elasticsearch;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import twitter4j.Status;

/**
 * Groups tweets into weekly indexes.
 *
 * Created by david on 2015-08-21.
 */
public class WeeklyIndexNameHeaderUpdater implements Processor {
    private final String indexType;

    public WeeklyIndexNameHeaderUpdater(String indexType) {
        this.indexType = indexType;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Status tweet = exchange.getIn().getBody(Status.class);
        String indexName = new DateTime(tweet.getCreatedAt()).withDayOfWeek(DateTimeConstants.MONDAY)
                .toString(String.format("'%s-'yyyy-MM-dd", indexType));

        exchange.getIn().setHeader("indexName", indexName);
        exchange.getIn().setHeader("indexType", indexType);
    }
}
