package com.skyfree.storm.redis;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/13 11:36
 */
public class RedisTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();   

        
        builder.setSpout("redis_spout", new RedisSpout(), 2);
        builder.setBolt("redis_bolt", new RedisBolt("127.0.0.1", 6379), 2).shuffleGrouping("redis_spout");

        Config config = new Config();

        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("redis_topology", config, builder.createTopology());

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println("Thread interrupted exception:" + e);
        }

        cluster.killTopology("redis_topology");
        cluster.shutdown();
    }
}
