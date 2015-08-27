package com.kaviddiss.twittercamel.elasticsearch;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by david on 2015-08-22.
 */
public class ElasticSearchSearchHitConverter implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {
        SearchHit hit = exchange.getIn().getBody(SearchHit.class);

        // Convert Elasticsearch documents to Maps before serializing to JSON:
        Map<String, Object> map = new HashMap<String, Object>(hit.sourceAsMap());
        map.put("score", hit.score());
        exchange.getIn().setBody(map);
    }
}
