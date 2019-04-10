package com.gx.test;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ClusterDemo {

    @Test
    public void test1() throws UnknownHostException {

        //指定ES集群cluster.name: my-application
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.26.130"), 9300));

        ClusterHealthResponse healths = client.admin().cluster().prepareHealth().get();
        String clusterName = healths.getClusterName();
        System.out.println("clusterName=" + clusterName);

        int numberOfDataNodes = healths.getNumberOfDataNodes();
        System.out.println("numberOfDataNodes=" + numberOfDataNodes);

        int numberOfNodes = healths.getNumberOfNodes();
        System.out.println("numberOfNodes" + numberOfNodes);
        for (ClusterIndexHealth health : healths.getIndices().values()) {
            String index = health.getIndex();
            int numberOfShards = health.getNumberOfShards();
            int numberOfReplicas = health.getNumberOfReplicas();
            System.out.printf("index=%s,numberOfShards=%d,numberOfReplicas=%s\n",index,numberOfShards,numberOfReplicas);
            ClusterHealthStatus status = health.getStatus();
            System.out.println(status.toString());
        }
    }
}
