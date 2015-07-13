package com.skyfree.storm.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.util.Map;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/13 11:28
 */
@SuppressWarnings("unused")
public class RedisOperations implements Serializable {

    private static final long serialVersionUID = 1L;

    private Jedis client = null;

    public RedisOperations(String redisIp, int port) {
        client = new Jedis(redisIp, port);
    }

    /**
     * 将记录插入到redis中
     *
     * @param record
     * @param id
     */
    public void insert(Map<String, Object> record, String id) {
        try {
            client.set(id, new ObjectMapper().writeValueAsString(record));
        } catch (JsonProcessingException e) {
            System.out.println("Record not persisted into datastore");
        }
    }
}
