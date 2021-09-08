package com.zz.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EsRestClientTest {

    public static void main(String[] args) throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
//        Map<String, Object> jsonMap = new HashMap<>();
//        jsonMap.put("user", "kimchy");
//        double amount = Math.pow(123,20);
//        System.out.println(amount);
//        jsonMap.put("income", amount);
//        jsonMap.put("message", "trying out Elasticsearch");
//        IndexRequest indexRequest = new IndexRequest("test_zz_2").id("1").source(jsonMap);
//        client.index(indexRequest, RequestOptions.DEFAULT);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("test_zz_2");
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            Map<String, Object> map = hit.getSourceAsMap();
            System.out.println(map);
            // do something with the SearchHit
        }
        client.close();
    }
}
