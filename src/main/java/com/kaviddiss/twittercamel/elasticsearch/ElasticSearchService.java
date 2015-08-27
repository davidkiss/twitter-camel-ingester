package com.kaviddiss.twittercamel.elasticsearch;

import org.apache.camel.Body;
import org.apache.camel.Header;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by david on 2015-08-22.
 */
public class ElasticSearchService {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchService.class);
    private static final int KEEP_ALIVE_MILLIS = 600000;

    private final Client client;
    private final String indexType;
    private final int batchSize;

    @Autowired
    public ElasticSearchService(Client client, String indexType, int batchSize) {
        this.indexType = indexType;
        this.batchSize = batchSize;
        this.client = client;
    }

    public SearchHitIterator search(@Body String query, @Header(value = "queryField") String queryField, @Header(value = "maxSize") int maxSize) {
        boolean scroll = maxSize > batchSize;
        LOG.info("Executing {} on index type: '{}' with query: '{}' and max: {}", scroll ? "scan & scroll" : "search", indexType, query, maxSize);
        QueryBuilder qb = termQuery(queryField, query);

        long startTime = System.currentTimeMillis();
        SearchResponse response = scroll ? prepareSearchForScroll(maxSize, qb) : prepareSearchForRegular(maxSize, qb);
        return new SearchHitIterator(client, response, scroll, maxSize, KEEP_ALIVE_MILLIS, startTime);
    }

    private SearchResponse prepareSearchForRegular(int maxSize, QueryBuilder qb) {
        SearchResponse response;
        response = client.prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTypes(indexType)
                .setQuery(qb)
                .setFrom(0).setSize(maxSize).setExplain(true)
                .execute().actionGet();
        return response;
    }

    private SearchResponse prepareSearchForScroll(int maxSize, QueryBuilder qb) {
        SearchResponse response;
        response = client.prepareSearch()
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setTypes(indexType)
                .setQuery(qb)
                .setSize(Math.min(maxSize, batchSize))
                .execute().actionGet();
        return response;
    }
}
