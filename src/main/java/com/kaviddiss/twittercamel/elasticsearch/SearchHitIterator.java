package com.kaviddiss.twittercamel.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by david on 2015-08-26.
 */
public class SearchHitIterator implements Iterator<SearchHit> {
    private static final Logger LOG = LoggerFactory.getLogger(SearchHitIterator.class);

    private SearchResponse scrollResp;
    private final Client client;
    private final int maxSize;
    private final long keepAliveMillis;
    private final boolean scrollEnabled;
    private final long startTime;

    private Iterator<SearchHit> hitIterator;
    private int counter;
    private boolean hasScrolled;

    public SearchHitIterator(Client client, SearchResponse scrollResp, boolean scrollEnabled, int maxSize, long keepAliveMillis, long startTime) {
        this.client = client;
        this.scrollEnabled = scrollEnabled;
        this.maxSize = maxSize < 0 ? 0 : maxSize;
        this.keepAliveMillis = keepAliveMillis;
        this.startTime = startTime;
        updateScrollResp(scrollResp);
    }

    private void updateScrollResp(SearchResponse scrollResp) {
        this.scrollResp = scrollResp;
        this.hitIterator = scrollResp.getHits().iterator();
        LOG.info("Got response with {} hits, total hits: {}, counter: {}, time elapsed: {}ms", scrollResp.getHits().getHits().length, scrollResp.getHits().getTotalHits(), counter, (System.currentTimeMillis() - startTime));
    }

    @Override
    public boolean hasNext() {
        if (!hitIterator.hasNext()) {
            scroll();
        }

        // todo test logic when there's no result to query in the index
        boolean hasNext = (hitIterator.hasNext() || (!hasScrolled && scrollEnabled)) && counter < maxSize;
        if (!hasNext) {
            LOG.info("Results found: {} - in {}s", counter, (System.currentTimeMillis() - startTime) / 1000);
        }
        return hasNext;
    }

    @Override
    public SearchHit next() {
        if (counter == maxSize) {
            throw new NoSuchElementException("Reached requested size: " + maxSize);
        }

        if (!hitIterator.hasNext()) {
            throw new NoSuchElementException();
        }
        counter++;
        return hitIterator.next();
    }

    private void scroll() {
        if (scrollEnabled) {
            long t0 = System.currentTimeMillis();
            SearchResponse searchResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(keepAliveMillis))
                    .execute()
                    .actionGet();
            LOG.info("Got batch in {}s", (System.currentTimeMillis() - t0) / 1000);
            updateScrollResp(searchResp);
            hasScrolled = true;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
