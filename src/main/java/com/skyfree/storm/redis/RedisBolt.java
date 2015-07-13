package com.skyfree.storm.redis;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/13 11:32
 */
public class RedisBolt implements IBasicBolt {
    private static final long serialVersionUID = 1L;
    private RedisOperations operations = null;
    private String redisIP = null;
    private int port;

    public RedisBolt(String redisIP, int port) {
        this.redisIP = redisIP;
        this.port = port;
    }

    public void prepare(Map map, TopologyContext topologyContext) {
        operations = new RedisOperations(this.redisIP, this.port);
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Map<String, Object> record = new HashMap<String, Object>();

        record.put("firstName", tuple.getValueByField("firstName"));
        record.put("lastName", tuple.getValueByField("lastName"));
        record.put("companyName", tuple.getValueByField("companyName"));

        operations.insert(record, UUID.randomUUID().toString());
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
