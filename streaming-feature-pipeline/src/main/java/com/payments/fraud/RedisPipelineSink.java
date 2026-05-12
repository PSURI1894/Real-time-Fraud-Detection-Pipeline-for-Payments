package com.payments.fraud;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Set;

public class RedisPipelineSink extends RichSinkFunction<String> {
    private transient JedisCluster jedisCluster;

    @Override
    public void open(Configuration parameters) throws Exception {
        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort("redis-cluster-01.fraud.local", 6379));
        jedisCluster = new JedisCluster(nodes);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        jedisCluster.setex("entity:card_id:" + value, 60, "1");
    }

    @Override
    public void close() throws Exception {
        if (jedisCluster != null) {
            jedisCluster.close();
        }
    }
}
