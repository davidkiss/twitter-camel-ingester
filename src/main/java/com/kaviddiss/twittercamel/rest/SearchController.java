package com.kaviddiss.twittercamel.rest;

import com.kaviddiss.twittercamel.CamelRouter;
import org.apache.camel.ProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by david on 2015-08-22.
 */
@Controller
public class SearchController {
    private static final Logger LOG = LoggerFactory.getLogger(SearchController.class);

    @Autowired
    private ProducerTemplate producerTemplate;

    @RequestMapping(value = "/tweet/search", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String tweetSearch(@RequestParam("q") String query,
                              @RequestParam(value = "max") int maxSize) {
        LOG.info("Tweet search request received with query: {} and max: {}", query, maxSize);
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("queryField", "content");
        headers.put("maxSize", maxSize);
        producerTemplate.asyncRequestBodyAndHeaders(CamelRouter.TWEET_SEARCH_URI, query, headers);
        return "Request is queued";
    }
}
