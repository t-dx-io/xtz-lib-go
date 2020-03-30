package cockroach

import (
	"time"

	common_model "github.com/t-dx/tg-blocksd/pkg/common/model"
	"github.com/t-dx/tg-blocksd/pkg/helper"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"
)

type transaction struct {
	ID                   string              `db:"id"`
	Hash                 string              `db:"hash"`
	Index                uint64              `db:"idx"`
	BlockNumber          *int64              `db:"block_number"`
	SourceAddress        *string             `db:"addr_from"`
	DestinationAddress   *string             `db:"addr_to"`
	Amount               *string             `db:"amount"`
	Fee                  *string             `db:"fee"`
	Counter              *string             `db:"counter"`
	Status               common_model.Status `db:"status"`
	RawTransaction       *string             `db:"rawtx"`
	Pinned               bool                `db:"pinned"`
	Broadcasted          bool                `db:"broadcasted"`
	Message              *string             `db:"message"`
	Timestamp            *time.Time          `db:"timestamp"`
	CreatedAt            *time.Time          `db:"created_at"`
	CreatedAtBlockNumber *uint64             `db:"created_at_block"`
	BroadcastedAtBlock   *uint64             `db:"broadcasted_at_block"`
}

func toModelTransaction(t *transaction) *model.Transaction {
	return &model.Transaction{
		ID:                   t.ID,
		Hash:                 t.Hash,
		Index:                t.Index,
		BlockNumber:          helper.BlockNumberPtrToUint64Ptr(t.BlockNumber),
		SourceAddress:        t.SourceAddress,
		DestinationAddress:   t.DestinationAddress,
		Amount:               helper.StringPtrToBigInt(t.Amount),
		Fee:                  helper.StringPtrToBigInt(t.Fee),
		Counter:              helper.StringPtrToBigInt(t.Counter),
		Status:               common_model.FromStatus(t.Status),
		RawTransaction:       t.RawTransaction,
		Pinned:               t.Pinned,
		Broadcasted:          t.Broadcasted,
		Message:              t.Message,
		Timestamp:            t.Timestamp,
		CreatedAt:            t.CreatedAt,
		CreatedAtBlockNumber: t.CreatedAtBlockNumber,
		BroadcastedAtBlock:   t.BroadcastedAtBlock,
	}
}

func toModelTransactions(legacyTransactions []*transaction) []*model.Transaction {
	var transactions = []*model.Transaction{}
	for _, legacyTransaction := range legacyTransactions {
		transactions = append(transactions, toModelTransaction(legacyTransaction))
	}
	return transactions
}
