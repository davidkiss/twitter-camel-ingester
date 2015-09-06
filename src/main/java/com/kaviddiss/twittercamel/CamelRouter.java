package com.kaviddiss.twittercamel;

import com.kaviddiss.twittercamel.camel.ListAggregationStrategy;
import com.kaviddiss.twittercamel.elasticsearch.ElasticSearchSearchHitConverter;
import com.kaviddiss.twittercamel.elasticsearch.ElasticSearchService;
import com.kaviddiss.twittercamel.elasticsearch.ElasticSearchTweetConverter;
import com.kaviddiss.twittercamel.elasticsearch.WeeklyIndexNameHeaderUpdater;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.elasticsearch.ElasticsearchComponent;
import org.apache.camel.component.elasticsearch.ElasticsearchEndpoint;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by david on 2015-08-21.
 */
@Component
public class CamelRouter extends RouteBuilder {
    private String ES_TWEET_INDEXER_ENDPOINT = "direct:tweet-indexer-ES";
    private String ES_TWEET_INDEX_TYPE = "tweet";
    public static final String TWEET_SEARCH_URI = "vm:tweetSearch";

    @Value("${elasticsearch.tweet.uri}")
    private String elasticsearchTweetUri;
    @Value("${elasticsearch.scrollBatchSize}")
    private int searchBatchSize;
    private ElasticSearchService esTweetService;

    @PostConstruct
    public void initCamelContext() throws Exception {
        // shutdown the producer first that reads from the twitter sample feed:
        getContext().getShutdownStrategy().setShutdownRoutesInReverseOrder(false);
        // wait max 5 seconds for camel to stop:
        getContext().getShutdownStrategy().setTimeout(5L);

        initESTweetService();
    }

    private void initESTweetService() {
        ElasticsearchComponent elasticsearchComponent = new ElasticsearchComponent(getContext());
        getContext().addComponent("elasticsearch", elasticsearchComponent);
        ElasticsearchEndpoint esTweetEndpoint = (ElasticsearchEndpoint) getContext().getEndpoint(elasticsearchTweetUri);
        esTweetService = new ElasticSearchService(esTweetEndpoint.getClient(), ES_TWEET_INDEX_TYPE, searchBatchSize);
    }

    @Override
    public void configure() throws Exception {
        from("twitter://streaming/sample?type=EVENT&consumerKey={{twitter4j.oauth.consumerKey}}&consumerSecret={{twitter4j.oauth.consumerSecret}}&accessToken={{twitter4j.oauth.accessToken}}&accessTokenSecret={{twitter4j.oauth.accessTokenSecret}}")
            .to(ES_TWEET_INDEXER_ENDPOINT)
        ;

        from(ES_TWEET_INDEXER_ENDPOINT)
            // groups tweets into separate indexes on a weekly basis to make it easier clean up old tweets:
            .process(new WeeklyIndexNameHeaderUpdater(ES_TWEET_INDEX_TYPE))
            // converts Twitter4j Tweet object into an elasticsearch document represented by a Map:
            .process(new ElasticSearchTweetConverter())
            // collects tweets into weekly batches based on index name:
            .aggregate(header("indexName"), new ListAggregationStrategy())
                // creates new batches every 2 seconds
                .completionInterval(2000)
                // makes sure the last batch will be processed before application shuts down:
                .forceCompletionOnStop()
            // inserts a batch of tweets to elasticsearch:
            .to(elasticsearchTweetUri)
            .log("Uploaded documents to ElasticSearch index ${headers.indexName}: ${body.size()}")
        ;

        from(TWEET_SEARCH_URI)
            .setHeader("CamelFileName", simple("tweet-${body}-${header.maxSize}-${date:now:yyyyMMddHHmmss}.txt"))
            // use an iterator to process search result instead of keeping results in memory:
            .split(method(esTweetService, "search"))
                .process(new ElasticSearchSearchHitConverter())
                .marshal(new JacksonDataFormat())
                .setBody(simple("${body}\n"))
                // write search results as json into a file under /tmp folder:
                .to("file:/tmp?fileExist=Append")
            .end()
            .log("Wrote search results to /tmp/${headers.CamelFileName}")
        ;
    }
}
