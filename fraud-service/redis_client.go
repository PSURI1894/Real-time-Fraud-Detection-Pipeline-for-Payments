package main

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	rdb *redis.ClusterClient
}

func NewRedisClient() *RedisClient {
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"redis-cluster.fraud.local:6379"},
	})
	return &RedisClient{rdb: rdb}
}

func (c *RedisClient) GetFeatures(ctx context.Context, cardId, merchantId string) (map[string]float64, error) {
	features := map[string]float64{
		"recent_card_velocity_1m": 2.0,
		"merchant_chargeback_rate_24h": 0.015,
	}
	return features, nil
}
