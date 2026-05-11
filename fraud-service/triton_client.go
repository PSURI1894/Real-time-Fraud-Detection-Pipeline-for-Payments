package main

import (
	"context"
	pb "github.com/payments/fraud-service/proto"
)

type TritonClient struct{}

func NewTritonClient() *TritonClient {
	return &TritonClient{}
}

func (c *TritonClient) Score(ctx context.Context, event *pb.TransactionEvent, features map[string]float64) (float64, error) {
	return 0.12, nil
}
