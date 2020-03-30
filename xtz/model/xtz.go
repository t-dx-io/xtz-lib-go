package model

import (
	"math/big"
	"time"
)

// Transaction maps an entry in the 'xtz_tx' database table.
// Nullable fields have pointer types.
type Transaction struct {
	ID                   string            `db:"id"`
	Hash                 string            `db:"hash"`
	Index                uint64            `db:"idx"`
	BlockNumber          *uint64           `db:"block_number"`
	SourceAddress        *string           `db:"addr_from"`
	DestinationAddress   *string           `db:"addr_to"`
	Amount               *big.Int          `db:"amount"`
	Fee                  *big.Int          `db:"fee"`
	Counter              *big.Int          `db:"counter"`
	Status               string            `db:"status"`
	RawTransaction       *string           `db:"rawtx"`
	Pinned               bool              `db:"pinned"`
	Broadcasted          bool              `db:"broadcasted"`
	Message              *string           `db:"error_message"`
	Timestamp            *time.Time        `db:"timestamp"`
	CreatedAt            *time.Time        `db:"created_at"`
	CreatedAtBlockNumber *uint64           `db:"created_at_block"`
	BroadcastedAtBlock   *uint64           `db:"broadcasted_at_block"`
	Attributes           map[string]string `db:"_"`
}

type BlockchainInfo struct {
	Height                uint64
	ConfirmationBlockHash string
}

// Balance represents the balance of a tezos address.
// Nullable fields have pointer types.
type Balance struct {
	Address        string
	BalanceAtBlock *big.Int
	BalanceAtTip   *big.Int
	Error          error
}

// Counter represents the counter of a tezos address.
// Nullable fields have pointer types.
type Counter struct {
	Address string
	Counter uint64
	Error   error
}

type Height struct {
	Height uint64
	Hash   string
}

type Fees struct {
	MinimalFees              *big.Int
	MinimalNanotezPerGasUnit *big.Int
	MinimalNanotezPerByte    *big.Int
}
