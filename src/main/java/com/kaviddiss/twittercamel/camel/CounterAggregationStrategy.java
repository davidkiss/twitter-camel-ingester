package com.kaviddiss.twittercamel.camel;

import org.apache.camel.Exchange;
import org.apache.camel.processor.aggregate.AggregationStrategy;

/**
 * Created by david on 2015-08-21.
 */
public class CounterAggregationStrategy implements AggregationStrategy {
    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        if (oldExchange == null) {
            newExchange.getIn().setBody(1);
            return newExchange;
        } else {
            int count = oldExchange.getIn().getBody(Integer.class);
            oldExchange.getIn().setBody(count + 1);
            return oldExchange;
        }
    }
}
