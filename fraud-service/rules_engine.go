package main

import (
	pb "github.com/payments/fraud-service/proto"
)

type RulesEngine struct{}

func NewRulesEngine() *RulesEngine {
	return &RulesEngine{}
}

func (re *RulesEngine) Evaluate(event *pb.TransactionEvent, features map[string]float64, score float64) (string, []string) {
	var triggeredRules []string

	if event.Amount > 10000.0 {
		triggeredRules = append(triggeredRules, "RULE_EXCESSIVE_AMOUNT")
		return "DECLINE", triggeredRules
	}

	if score > 0.85 {
		triggeredRules = append(triggeredRules, "RULE_HIGH_MODEL_RISK")
		return "DECLINE", triggeredRules
	} else if score > 0.5 {
		triggeredRules = append(triggeredRules, "RULE_MODERATE_MODEL_RISK")
		return "REVIEW", triggeredRules
	}

	return "APPROVE", triggeredRules
}
