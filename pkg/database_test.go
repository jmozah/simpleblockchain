package pkg

import (
	"testing"
)

type Result struct {
	output1 []int
	output2 int
	output3 int
	output4 string
}

func TestDatabase(t *testing.T) {

	for _, tc := range []struct {
		name    string
		input1  []int
		output1 []int
	}{
		{
			name:    "Single successful transaction, TX1 - (tr2 valid)",
			input1:  []int{3, 1, 5, 2, 10, 3, 15, 1, 2, 1, 2, 3, 3, 1, 2},
			output1: []int{3, 1, 4, 2, 13, 3, 13, 1, 1},
		},
		{
			name:    "Multiple successful transaction, TX2 - (tr2 valid, tr1 valid)",
			input1:  []int{3, 1, 5, 2, 10, 3, 15, 2, 2, 1, 2, 3, 3, 1, 2, 1, 2, 1, 11},
			output1: []int{3, 1, 15, 2, 2, 3, 13, 2, 1, 2},
		},
		{
			name:    "Single failing transaction that roll back, TX1 - (tr1 valid)",
			input1:  []int{3, 1, 5, 2, 10, 3, 15, 1, 1, 1, 2, 0},
			output1: []int{3, 1, 15, 2, 10, 3, 15, 1, 1},
		},
		{
			name:    "Multiple transactions that restore consistency, TX2 - (tr1 valid, tr2 valid)",
			input1:  []int{3, 1, 5, 2, 10, 3, 15, 2, 1, 2, 1, 11, 2, 1, 2, 3, 3, 1, 2},
			output1: []int{3, 1, 15, 2, 2, 3, 13, 2, 1, 2},
		},
		{
			name:    "Multiple transaction with one invalid transaction that is ignored, TX3 - (tr1 valid, tr1 invalid, tr2 valid)",
			input1:  []int{3, 1, 5, 2, 10, 3, 15, 3, 1, 2, 1, 11, 1, 2, 3, 5, 2, 1, 2, 3, 3, 1, 2},
			output1: []int{3, 1, 15, 2, 2, 3, 13, 2, 1, 3},
		},
		{
			name:    "Multiple transaction with one invalid transaction containing two transfers that is ignored, TX3 - (tr1 valid, tr2 invalid, tr2 valid)",
			input1:  []int{3, 1, 5, 2, 10, 3, 15, 3, 1, 2, 1, 11, 2, 2, 3, 5, 3, 1, 2, 2, 1, 2, 3, 3, 1, 2},
			output1: []int{3, 1, 15, 2, 2, 3, 13, 2, 1, 3},
		},
		{
			name:    "Multiple transaction with one invalid transaction containing two transfers that is ignored, TX3 - (tr1 valid, tr2 invalid, tr2 valid)",
			input1:  []int{3, 1, 5, 2, 10, 3, 15, 3, 1, 2, 1, 11, 2, 2, 3, 20, 3, 1, 2, 2, 1, 2, 3, 3, 1, 2},
			output1: []int{3, 1, 15, 2, 2, 3, 13, 2, 1, 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := public_tester(tc.input1, 0, 0, tc.name)
			if result.output2 != len(tc.output1) {
				t.Fatalf("expect length %d got %d length", len(tc.output1), result.output2)
			}

			for i, expected := range tc.output1 {
				if tc.output1[i] != expected {
					t.Fatalf("expected %d, got %d", expected, tc.output1[i])
				}
			}
		})
	}
}

func public_tester(input1 []int, input2 int, input3 int, input4 string) *Result {

	p := 0
	p++
	balances := make(map[int]int)
	var test_balances []AccountBalances

	for i := input1[0]; i > 0; i-- {
		account := input1[p]
		p++
		balance := input1[p]
		p++
		balances[account] = balance
		test_balances = append(test_balances, AccountBalances{accountId: account, balance: balance})
	}

	db := CreateDatabase(test_balances)
	var transactions []Transaction
	for i := input1[p]; i > 0; i-- {
		var transaction Transaction
		p++
		for j := input1[p]; j > 0; j-- {
			p++
			from := input1[p]
			p++
			to := input1[p]
			p++
			amount := input1[p]
			transfer := Transfer{
				from:   from,
				to:     to,
				amount: amount,
			}
			transaction.transfers = append(transaction.transfers, transfer)
		}
		transactions = append(transactions, transaction)
		_ = db.PushTransaction(&transaction)
	}

	_ = db.Settle()

	candidate_balances := db.GetBalances()
	candidate_applied_transactions := db.GetAppliedTransactions()
	var outputVector []int

	outputVector = append(outputVector, len(candidate_balances))

	for i, _ := range test_balances {
		accountId := test_balances[i].accountId
		outputVector = append(outputVector, accountId)
		outputVector = append(outputVector, candidate_balances[accountId])
	}

	outputVector = append(outputVector, len(candidate_applied_transactions))
	for i := 0; i < len(candidate_applied_transactions); i++ {
		outputVector = append(outputVector, candidate_applied_transactions[i])
	}

	return &Result{
		output1: outputVector,
		output2: len(outputVector),
		output3: input3,
		output4: input4,
	}
}
