package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	pb "github.com/payments/fraud-service/proto"
)

type server struct {
	pb.UnimplementedFraudServiceServer
}

func (s *server) CheckTransaction(ctx context.Context, req *pb.FraudRequest) (*pb.FraudDecision, error) {
	log.Printf("Received transaction: Card=%s Amount=%.2f", req.Event.CardId, req.Event.Amount)
	return &pb.FraudDecision{
		Decision: "APPROVE",
		Score:    0.05,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterFraudServiceServer(s, &server{})
	log.Printf("Fraud Service listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
