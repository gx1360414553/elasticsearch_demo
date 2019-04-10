package com.gx.test;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ExecutionException;


public class EsDemo {

    @Test
    public void test1() throws UnknownHostException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.229.128"), 9300));
//        PUT /lib/user/1
//        {
//            "first_name":  "gao",
//                "last_name" : "xiong",
//                "age" : 18,
//                "about" : "I like to collect rock albums",
//                "interests" : ["music","dance"]
//        }
        //数据查询
        GetResponse response = client.prepareGet("lib", "user", "1").execute().actionGet();

        //得到查询出的数据
        System.out.println(response.getSourceAsString());

        client.close();

    }

    @Test
    public void test2() throws IOException {
//        PUT /index1
//        {
//            "settings": {
//            "number_of_shards": 3,
//                    "number_of_replicas": 0
//        },
//            "mappings": {
//            "blog":{
//                "properties": {
//                    "id":{
//                        "type":"long"
//                    },
//                    "title":{
//                        "type": "text",
//                                "analyzer": "ik_max_word"
//                    },
//                    "content":{
//                        "type": "text",
//                                "analyzer": "ik_max_word"
//                    },
//                    "postdate":{
//                        "type": "date"
//                    },
//                    "url":{
//                        "type": "text"
//                    }
//                }
//            }
//        }
//        }
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.229.128"), 9300));

        //添加文档
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                .field("id", "1")
                .field("title", "Java设计模式之装饰者模式")
                .field("content", "在不改变原类文件和使用继承的情况下，动态的扩展一个类的功能!")
                .field("postdate", "2018-05-02")
                .field("url", "csdn.net/79263564")
                .endObject();
        IndexResponse response = client.prepareIndex("index1", "blog", "10").setSource(doc).get();

        System.out.println(response.status());
        client.close();
    }

    @Test
    public void test3() throws UnknownHostException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));
        //删除文档
        DeleteResponse response = client.prepareDelete("index1", "blog", "10").get();

        System.out.println(response.status());
        client.close();
    }

    @Test
    public void test4() throws IOException, ExecutionException, InterruptedException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.229.128"), 9300));
        //更新文档
        UpdateRequest request = new UpdateRequest();
        request.index("index1").type("blog").id("10")
                .doc(
                        XContentFactory.jsonBuilder().startObject()
                                .field("title", "单例设计模式")
                                .endObject()
                );
        UpdateResponse response = client.update(request).get();
        System.out.println(response.status());
        client.close();
    }

    @Test
    public void test5() throws IOException, ExecutionException, InterruptedException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.229.128"), 9300));
        //upsert修改没有就新增
        IndexRequest request1 = new IndexRequest("index1", "blog", "8")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "2")
                        .field("title", "工厂模式")
                        .field("content", "静态工厂，实例工厂!")
                        .field("postdate", "2018-05-25")
                        .field("url", "csdn.net/79212456")
                        .endObject()
                );
        UpdateRequest request2 = new UpdateRequest("index1", "blog", "8").doc(
                XContentFactory.jsonBuilder().startObject()
                        .field("title", "策略模式")
                        .endObject()
        ).upsert(request1);
        UpdateResponse response = client.update(request2).get();
        System.out.println(response.status());
        client.close();
    }

    //mget查询
    @Test
    public void test6() throws UnknownHostException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.229.128"), 9300));

        MultiGetResponse responses = client.prepareMultiGet()
                .add("index1", "blog", "8", "10")
                .add("lib3", "user", "1", "2", "3").get();

        for (MultiGetItemResponse item : responses) {
            GetResponse response = item.getResponse();
            if (response != null && response.isExists()) {
                System.out.println(response.getSourceAsString());
            }

        }

    }

    //bulk批量操作
    @Test
    public void test7() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.229.128"), 9300));

        BulkRequestBuilder builder = client.prepareBulk();

        //批量添加
        builder.add(client.prepareIndex("lib2", "books", "8")
                .setSource(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("title", "python")
                                .field("price", 999)
                                .endObject()
                )
        );
        builder.add(client.prepareIndex("lib2", "books", "9")
                .setSource(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("title", "VR")
                                .field("price", 889)
                                .endObject()
                )
        );
        BulkResponse responses = builder.get();
        System.out.println(responses.status());
        if (responses.hasFailures()) {
            System.out.println("失败了");
        }

    }

    //查询删除
    @Test
    public void test8() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.229.128"), 9300));

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
                .newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("title", "工厂"))
                .source("index1")
                .get();

        long counts = response.getDeleted();
        System.out.println(counts);

    }

    //match_all
    @Test
    public void test9() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));

        MatchAllQueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchResponse sr = client.prepareSearch("lib")
                .setQuery(qb)
                .setSize(3).get();

        SearchHits hits = sr.getHits();

        for (SearchHit hit : hits) {

            System.out.println(hit.getSourceAsString());
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            for (String key : sourceAsMap.keySet()) {

                System.out.println(key + "=" + sourceAsMap.get(key));

            }
        }

    }

    //match_query
    @Test
    public void test10() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));

        MatchQueryBuilder builder = QueryBuilders.matchQuery("interests", "piano");

        SearchResponse response = client.prepareSearch("lib")
                .setQuery(builder)
                .setSize(3)
                .get();

        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {

            System.out.println(hit.getSourceAsString());
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            for (String key : sourceAsMap.keySet()) {

                System.out.println(key + "=" + sourceAsMap.get(key));

            }
        }

    }

    //multiMatchQuery
    @Test
    public void test11() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));
        QueryBuilder builder = QueryBuilders.multiMatchQuery("piano", "interests", "about");

        SearchResponse response = client.prepareSearch("lib")
                .setQuery(builder)
                .setSize(3)
                .get();

        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {

            System.out.println(hit.getSourceAsString());
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            for (String key : sourceAsMap.keySet()) {

                System.out.println(key + "=" + sourceAsMap.get(key));

            }
        }
    }

    //term Query
    @Test
    public void test12() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));
        QueryBuilder builder = QueryBuilders.termQuery("about", "play basketball");

        SearchResponse response = client.prepareSearch("lib")
                .setQuery(builder)
                .setSize(3)
                .get();

        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {

            System.out.println(hit.getSourceAsString());
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            for (String key : sourceAsMap.keySet()) {

                System.out.println(key + "=" + sourceAsMap.get(key));

            }
        }
    }

    //terms Query
    @Test
    public void test13() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));
        QueryBuilder builder = QueryBuilders.termsQuery("interests", "music", "dance");

        SearchResponse response = client.prepareSearch("lib")
                .setQuery(builder)
                .setSize(3)
                .get();

        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {

            System.out.println(hit.getSourceAsString());
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            for (String key : sourceAsMap.keySet()) {

                System.out.println(key + "=" + sourceAsMap.get(key));

            }
        }
    }

    //各种查询
    @Test
    public void test14() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));
        //range查询
        //QueryBuilder builder = QueryBuilders.rangeQuery("postdate").format("2018-04-25").to("2018-05-25").format("yyyy-MM-dd").includeUpper(false);
        //prefix查询
        //QueryBuilder builder = QueryBuilders.prefixQuery("title","工厂");
        //wildcard通配符查询 *多位 ?一位
        //QueryBuilder builder = QueryBuilders.wildcardQuery("title","工厂*");
        //fuzzy查询 模糊查询
        //QueryBuilder builder = QueryBuilders.fuzzyQuery("interests","pino");
        //type查询
        //QueryBuilder builder = QueryBuilders.typeQuery("blog");
        //ids查询
        QueryBuilder builder = QueryBuilders.idsQuery().addIds("8");
        SearchResponse response = client.prepareSearch("index1")
                .setQuery(builder)
                .get();

        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {

            System.out.println(hit.getSourceAsString());
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            for (String key : sourceAsMap.keySet()) {

                System.out.println(key + "=" + sourceAsMap.get(key));

            }
        }
    }

    //聚合查询
    @Test
    public void test15() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));
        //最大值
//        AggregationBuilder agg = AggregationBuilders.max("aggMax").field("age");
//        SearchResponse response = client.prepareSearch("lib").addAggregation(agg).get();
//        Max max = response.getAggregations().get("aggMax");
//        System.out.println(max.getValue());

        //最小值
//        AggregationBuilder agg = AggregationBuilders.min("aggMin").field("age");
//        SearchResponse response = client.prepareSearch("lib").addAggregation(agg).get();
//        Min min = response.getAggregations().get("aggMin");
//        System.out.println(min.getValue());
        //平均值
//        AggregationBuilder agg = AggregationBuilders.avg("aggAvg").field("age");
//        SearchResponse response = client.prepareSearch("lib").addAggregation(agg).get();
//        Avg avg = response.getAggregations().get("aggAvg");
//        System.out.println(avg.getValue());

        //总和
//        AggregationBuilder agg = AggregationBuilders.sum("aggSum").field("age");
//        SearchResponse response = client.prepareSearch("lib").addAggregation(agg).get();
//        Sum sum = response.getAggregations().get("aggSum");
//        System.out.println(sum.getValue());

        //有几种值得个数
        AggregationBuilder agg = AggregationBuilders.cardinality("aggCardinality").field("age");
        SearchResponse response = client.prepareSearch("lib").addAggregation(agg).get();
        Cardinality cardinality = response.getAggregations().get("aggCardinality");
        System.out.println(cardinality.getValue());
    }

    //query string
    @Test
    public void test16() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));
        //查找含有
        //QueryBuilder builder = QueryBuilders.commonTermsQuery("title", "工厂");
        //查找所以字段含有 +是必须含有 -不含有
//        QueryBuilder builder = QueryBuilders.queryStringQuery("+music -piano");
        //满足其中一个条件就可以查出来
        QueryBuilder builder = QueryBuilders.simpleQueryStringQuery("+music -piano");
        SearchResponse response = client.prepareSearch("lib")
                .setQuery(builder)
                .get();

        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

        }
    }

    //组合查询
    @Test
    public void test17() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));

//        BoolQueryBuilder builder = QueryBuilders.boolQuery()
//                .must(QueryBuilders.matchQuery("interests", "music"))
//                .mustNot(QueryBuilders.matchQuery("interests", "piano"))
//                .should(QueryBuilders.rangeQuery("about").gte("basketball"))
//                .filter(QueryBuilders.rangeQuery("age").gte(11));
        //.filter(QueryBuilders.rangeQuery("date").gte("2018-11-15").format("yyyy-MM-dd"));

        //constantscore
        QueryBuilder builder = QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("interests", "music"))
                .mustNot(QueryBuilders.matchQuery("interests", "piano"))
                .should(QueryBuilders.rangeQuery("about").gte("basketball"))
                .filter(QueryBuilders.rangeQuery("age").gte(11)));
        SearchResponse response = client.prepareSearch("lib")
                .setQuery(builder)
                .get();

        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

        }
    }

    //分组聚合
    @Test
    public void test18() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));

        AggregationBuilder builder = AggregationBuilders.terms("terms").field("age");

        SearchResponse response = client.prepareSearch("lib").addAggregation(builder).execute().actionGet();


        Terms terms = response.getAggregations().get("terms");
        for (Terms.Bucket bucket : terms.getBuckets()) {
            System.out.println(bucket.getKey() + ": " + bucket.getDocCount());
        }

    }


    //filter聚合
    @Test
    public void test19() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));


        QueryBuilder query = QueryBuilders.termQuery("age", 14);

        AggregationBuilder agg = AggregationBuilders.filter("filter", query);

        SearchResponse response = client.prepareSearch("lib").addAggregation(agg).execute().actionGet();

        Filter filter = response.getAggregations().get("filter");

        System.out.println(filter.getDocCount());
    }

    //filters聚合
    @Test
    public void test20() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));

        AggregationBuilder agg = AggregationBuilders.filters("filters",
                new FiltersAggregator.KeyedFilter("music", QueryBuilders.termQuery("interests", "music")),
                new FiltersAggregator.KeyedFilter("piano2", QueryBuilders.termQuery("interests", "piano2")));

        SearchResponse response = client.prepareSearch("lib").addAggregation(agg).execute().actionGet();

        Filters filters = response.getAggregations().get("filters");
        for (Filters.Bucket bucket : filters.getBuckets()) {
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount());
        }

    }

    //range聚合
    @Test
    public void test21() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));

        AggregationBuilder agg = AggregationBuilders.range("range")
                .field("age")
                .addUnboundedTo(15)//(,to) 前面无边界到15(不含)结束
                .addRange(10, 15)//[from,to]
                .addUnboundedFrom(15);//[from,to)后面无边界从15(含)开始
        SearchResponse response = client.prepareSearch("lib").addAggregation(agg).execute().actionGet();

        Range range = response.getAggregations().get("range");
        for (Range.Bucket bucket : range.getBuckets()) {
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount());
        }
//        *-50.0:5
//        25.0-50.0:0
//        25.0-*:0
    }

    //missing聚合 统计为null数量
    @Test
    public void test22() throws IOException {
        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));

        AggregationBuilder agg = AggregationBuilders.missing("missing").field("age");

        SearchResponse response = client.prepareSearch("lib").addAggregation(agg).execute().actionGet();

        Aggregation aggregation = response.getAggregations().get("missing");
        System.out.println(aggregation.toString());
    }
}