package pkg

import (
	"fmt"
	"sync"
)

// Database contains all the accounts and their respective settled balances at any given point in time.
// It should always be clean i.e. properly settled
// This also contains spme of the transaction state that is cleaned after every settlement
type Database struct {
	accounts  map[int]int                     // account id as key and amount as value
	accountMu sync.RWMutex                    // mutex for preventing the accounts data structure

	// Intermediate states of the Accounts
	transactionId       int                   // the global transaction Id
	accountState        map[int]*AccountState // key is accountId and value is the state transactions affecting the account
	accountStateLock    sync.RWMutex          // mutex to make sure that the above DS is protected
	appliedTransactions []int                 // stores all the applied transaction id
}

// AccountState is a structure to maintain intermediate account state while applying transactions
type AccountState struct {
	initialBalance int
	transactions   map[int]int // key is txId and value is list if transfers
}

// AccountBalances is a structure to transfer an account and its related detals during database creation
type AccountBalances struct {
	accountId int
	balance   int
}

// Transfer indicated a movement if money from one account to another
type Transfer struct {
	from   int // account from which money should be moved
	to     int // account to which money should be deposited
	amount int // The amount of money that should be transferred
}

// Transaction contains one or more transfers which need to be applied atomically to the
// database. i.e. all the transfers to the transaction should be either applied or not.
type Transaction struct {
	transfers []Transfer // The set of transfers that constitute this transaction
}

// CreateDatabase create the database instance with the given state of the accounts and their balances
// If an acount is already found in the database, the old amount will be overwritten
func CreateDatabase(accountsToAdd []AccountBalances) *Database {

	// create the database object
	database := &Database{
		accounts:      make(map[int]int),
		accountState:  make(map[int]*AccountState),
		transactionId: 0,
	}

	// populate the given accounts and their balances
	for _, accountBalances := range accountsToAdd {
		if _, ok := database.accounts[accountBalances.accountId]; ok {
			// if the accountis already present.. overwrite it with the new data
			database.accounts[accountBalances.accountId] = accountBalances.balance
			continue
		}
		database.accounts[accountBalances.accountId] = accountBalances.balance
	}

	return database
}


// PushTransaction receives the transaction and store it in the account state store for the
// affected accounts along with the transaction references
func (d *Database) PushTransaction(transactionToPush *Transaction) error {

	// lock the account state while processing a transaction
	d.accountStateLock.Lock()
	defer d.accountStateLock.Unlock()

	// find the transaction Id and initialise the account state
	if d.accountState == nil {
		d.accountState = make(map[int]*AccountState)
	}
	d.transactionId++

	// check for the transaction input for sanity
	if transactionToPush == nil {
		return fmt.Errorf("invalid transacion")
	}
	if len(transactionToPush.transfers) == 0 {
		return fmt.Errorf("invalid number of tranfers in the transactions")
	}

	// collect the accounts that are affected by this transaction, both "from" and "to".
	// assumption is that both the from and to accounts should be present in the database,
	// otherwise the entire transaction is rejected (i.e. error will be returned)
	for _, transfer := range transactionToPush.transfers {

		// "from" accounts collection
		if bal, ok := d.accounts[transfer.from]; ok {
			// create the "from" account state if it is not present already and add the current amount at index 0
			if _, ok := d.accountState[transfer.from]; !ok {
				d.accountState[transfer.from] = &AccountState{
					transactions: make(map[int]int),
				}
				d.accountState[transfer.from].initialBalance = bal
			}
		} else {
			return fmt.Errorf("source account %d not present. Ignoring the entire transaction", transfer.from)
		}

		// "to" accounts collection
		if bal, ok := d.accounts[transfer.to]; ok {
			// create the "to" account state if it is not present already  and add the current amount at index 0
			if _, ok := d.accountState[transfer.to]; !ok {
				d.accountState[transfer.to] = &AccountState{
					transactions: make(map[int]int),
				}
				d.accountState[transfer.to].initialBalance = bal
			}
		} else {
			return fmt.Errorf("destnation account %d not present. Ignoring the entire transaction", transfer.to)
		}
	}

	// take the previous transaction values to apply the current transaction
	txBal := make(map[int]int)

	// apply the transfers
	for _, transfer := range transactionToPush.transfers {
		txBal[transfer.from] = txBal[transfer.from] - transfer.amount
		txBal[transfer.to] = txBal[transfer.to] + transfer.amount
	}

	// store the transaction result in the respective accounts states
	for accountId, bal := range txBal {
		d.accountState[accountId].transactions[d.transactionId] = bal
	}

	return nil
}

// Settle goes through all the state store and commits the balances to the affected accounts atomically
func (d *Database) Settle() error {

	// check if there is anything left to settle
	if d.accountState == nil {
		return fmt.Errorf("nothing to settele")
	}

	// protect this unsettled accounts by a lock until we collect and apply the transaction
	d.accountStateLock.Lock()
	d.accountMu.Lock()

	// clear all the state at the end whether the settlement is done or not
	defer func() {
		d.accountStateLock.Unlock()
		d.accountMu.Unlock()
		d.accountState = nil
		d.transactionId = 0
	}()

	// find all invalid transactions
	ignoredTransactions := d.getInvalidTransactions()

	// settle the accounts ignoring the invalid transactions
	appliedTx := make(map[int]bool)
	for accountId, accountState := range d.accountState {
		finalBalance := accountState.initialBalance
		for txId, bal := range accountState.transactions {
			if _, ok := ignoredTransactions[txId]; !ok {
				finalBalance += bal
				appliedTx[txId] = true
			}
		}
		d.accounts[accountId] = finalBalance
	}

	d.appliedTransactions = nil
	for txId, _ := range appliedTx {
		d.appliedTransactions = append(d.appliedTransactions, txId)
	}

	return nil
}

// GetBalances returns the accounts and their current balances
func (d *Database) GetBalances() map[int]int {
	return d.accounts
}

// GetAppliedTransactions returnes the indexes of the applied transactions
func (d *Database) GetAppliedTransactions() []int {
	return d.appliedTransactions
}



// getInvalidTransactions finds out transactions that will change the state in a invariant state
func (d *Database) getInvalidTransactions() map[int]bool {

	allInvalidTx := make(map[int]bool)

	// check if all the state are valid
	for _, accountState := range d.accountState {
		finalBalance := accountState.initialBalance
		for _, bal := range accountState.transactions {
			finalBalance += bal
		}

		var invalidTransactions []int
		if finalBalance < 0 {
			// find invalid transaction by iterationg from the back to remove the -ve balances
			balanceToRemove := finalBalance
			for txId := len(accountState.transactions); txId > 0; txId-- {
				bal := accountState.transactions[txId]
				if bal < 0 {
					invalidTransactions = append(invalidTransactions, txId)
					balanceToRemove -= bal
					if balanceToRemove > finalBalance {
						break
					}
				}
			}

			// add the invalid transaction to the global invalid transaction list
			for _, txId := range invalidTransactions {
				allInvalidTx[txId] = true
			}
		}
	}

	return allInvalidTx
}
