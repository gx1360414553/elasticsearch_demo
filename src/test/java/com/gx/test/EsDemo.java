package com.gx.test;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


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
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.229.128"), 9300));

        /*client.prepareSearch("lib3")
                .setQuery(qb)*/

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(new Runnable() {
            public void run() {
                for (int i = 0; i < 10000; i++) {
                    System.out.println(i);
                }
                System.out.println("end................");
            }
        });

    }
}
