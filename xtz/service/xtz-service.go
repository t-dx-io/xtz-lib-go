package service

import (
	"context"
	"time"

	"github.com/t-dx/tg-blocksd/internal/logger"
	common_model "github.com/t-dx/tg-blocksd/pkg/common/model"
	common_service "github.com/t-dx/tg-blocksd/pkg/common/service"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"

	"go.uber.org/zap"
)

type BroadcastReq struct {
	Network        string
	CustomerID     string
	RawTransaction string
}

type GetTransactionsByHashesReq struct {
	Network string
	Hashes  []string
}

type GetTransactionsByBlocksReq struct {
	Network   string
	Addresses []string
	FromBlock uint64
	ToBlock   uint64
	Limit     uint64
	Offset    uint64
}

type GetTransactionsByDatesReq struct {
	Network   string
	Addresses []string
	FromDate  time.Time
	ToDate    time.Time
	Limit     uint64
	Offset    uint64
}

// XTZer defines the tezos service API.
type XTZer interface {
	AddAddresses(ctx context.Context, req *AddAddressesReq) error
	Broadcast(ctx context.Context, req *BroadcastReq) (string, error)
	GetBlockchainInfo(ctx context.Context, req *GetBlockchainInfoReq) (*model.BlockchainInfo, error)
	GetEstimatedFee(ctx context.Context, req *GetEstimatedFeeReq) (*model.Fees, error)
	GetBalances(ctx context.Context, req *GetBalancesReq) ([]*model.Balance, error)
	GetCounters(ctx context.Context, req *GetCountersReq) ([]*model.Counter, error)
	GetTransactionsByHashes(ctx context.Context, req *GetTransactionsByHashesReq) ([]*model.Transaction, uint64, error)
	GetTransactionsByBlocks(ctx context.Context, req *GetTransactionsByBlocksReq) ([]*model.Transaction, uint64, uint64, error)
	GetTransactionsByDates(ctx context.Context, req *GetTransactionsByDatesReq) ([]*model.Transaction, uint64, uint64, error)
	GetRawTransactionHash(ctx context.Context, rawTransaction string) (string, error)
}

type Client interface {
	BroadcastTransaction(ctx context.Context, rawTransaction string) error
	GetEstimatedFee(ctx context.Context) (*model.Fees, error)
	GetBalances(ctx context.Context, addresses []string, blockNumber uint64) ([]*model.Balance, error)
	GetBlock(ctx context.Context, blockNumber uint64) (*common_model.Block, error)
	GetHeight(ctx context.Context) (*model.Height, error)
	GetCounters(ctx context.Context, addresses []string) ([]*model.Counter, error)
	GetRawTransactionHash(ctx context.Context, rawTransaction string) (string, error)
	GetTransactions(ctx context.Context, blockNumber uint64) ([]*model.Transaction, error)
}

type TransactionStore interface {
	CreateTransactions(ctx context.Context, transactions []*model.Transaction) error
	GetTransactions(ctx context.Context, hashes []string) ([]*model.Transaction, error)
	GetTransactionsBetweenBlocks(ctx context.Context, addresses []string, fromBlock, toBlock uint64, limit, offset uint64) ([]*model.Transaction, uint64, error)
	GetTransactionsBetweenDates(ctx context.Context, addresses []string, fromDate, toDate time.Time, limit, offset uint64) ([]*model.Transaction, uint64, error)
	MarkPinned(ctx context.Context, addresses []string) error
	Broadcast(ctx context.Context, transaction *model.Transaction) error
	GetPendingBroadcasts(ctx context.Context, broadcastedBeforeBlock, limit uint64) ([]*model.Transaction, error)
	UpdateBroadcast(ctx context.Context, hash string, status string, message string, broadcastedAtBlock uint64) error
	GetBroadcastsToGarbageCollect(ctx context.Context, beforeBlock uint64) ([]string, error)
	GarbageCollectBroadcasts(ctx context.Context, broadcastHashes []string) error
	GarbageCollectTransactions(ctx context.Context, beforeBlock uint64) error
	DumpPendingBroadcasts(ctx context.Context, limit, offset uint64, asOfSystemTime time.Time) ([]*model.Transaction, uint64, error)
	DumpPinnedTransactions(ctx context.Context, limit, offset uint64, asOfSystemTime time.Time) ([]*model.Transaction, uint64, error)
	DeleteBlockTransactions(ctx context.Context, blockNumber uint64) error
}

// XTZService is the tezos service handler.
type XTZService struct {
	addressStore         common_service.AddressStore
	blockStore           common_service.BlockStore
	chunkStore           common_service.ChunkStore
	transactionStore     TransactionStore
	broadcastTrailsStore common_service.BroadcastTrailsStore
	client               Client
	startBlock           uint64
}

// Verify XTZService satisfies the XTZer interface.
var _ XTZer = (*XTZService)(nil)

// NewXTZService returns a fresh tezos service instance.
func NewXTZService(addressStore common_service.AddressStore, blockStore common_service.BlockStore, chunkStore common_service.ChunkStore, transactionStore TransactionStore, broadcastTrailsStore common_service.BroadcastTrailsStore, client Client, startBlock uint64) *XTZService {
	return &XTZService{
		addressStore:         addressStore,
		blockStore:           blockStore,
		chunkStore:           chunkStore,
		transactionStore:     transactionStore,
		broadcastTrailsStore: broadcastTrailsStore,
		client:               client,
		startBlock:           startBlock,
	}
}

func (s *XTZService) AddAddresses(ctx context.Context, req *AddAddressesReq) error {
	return s.addressStore.CreateAddresses(ctx, req.Addresses)
}

func (s *XTZService) Broadcast(ctx context.Context, req *BroadcastReq) (string, error) {
	hash, err := s.client.GetRawTransactionHash(ctx, req.RawTransaction)
	if err != nil {
		return "", err
	}

	var blockNumber = s.startBlock
	block, err := s.blockStore.GetLastBlock(ctx)
	if err == nil {
		blockNumber = block.Number
	}

	err = s.transactionStore.Broadcast(ctx, &model.Transaction{Hash: hash, RawTransaction: &req.RawTransaction, Timestamp: &time.Time{}, CreatedAtBlockNumber: &blockNumber})
	if err != nil {
		return "", err
	}

	err = s.broadcastTrailsStore.InsertBroadcastTrails(ctx, []*common_model.BroadcastTrail{{
		Currency:        "XTZ",
		Action:          "store",
		TransactionHash: hash,
		BroadcastStatus: common_model.NEW.String(),
		Date:            time.Now().UTC(),
	}})
	if err != nil {
		logger.TechLog.Error(ctx, "unable to insert trail", zap.Error(err), zap.String("currency", "XTZ"), zap.String("action", "broadcast"), zap.String("transaction_hash", hash), zap.String("status", common_model.NEW.String()))
	}

	return hash, nil
}

func (s *XTZService) GetBlockchainInfo(ctx context.Context, req *GetBlockchainInfoReq) (*model.BlockchainInfo, error) {
	height, err := s.client.GetHeight(ctx)
	if err != nil {
		return nil, err
	}

	return &model.BlockchainInfo{
		Height:                height.Height,
		ConfirmationBlockHash: height.Hash,
	}, nil
}

func (s *XTZService) GetEstimatedFee(ctx context.Context, req *GetEstimatedFeeReq) (*model.Fees, error) {
	return s.client.GetEstimatedFee(ctx)
}

func (s *XTZService) GetBalances(ctx context.Context, req *GetBalancesReq) ([]*model.Balance, error) {
	return s.client.GetBalances(ctx, req.Addresses, req.BlockNumber)
}

func (s *XTZService) GetCounters(ctx context.Context, req *GetCountersReq) ([]*model.Counter, error) {
	return s.client.GetCounters(ctx, req.Addresses)
}

func (s *XTZService) GetTransactionsByHashes(ctx context.Context, req *GetTransactionsByHashesReq) ([]*model.Transaction, uint64, error) {
	transactions, err := s.transactionStore.GetTransactions(ctx, req.Hashes)
	if err != nil {
		return nil, 0, err
	}

	height, err := s.client.GetHeight(ctx)
	if err != nil {
		return nil, 0, err
	}

	return transactions, height.Height, nil
}

func (s *XTZService) GetTransactionsByBlocks(ctx context.Context, req *GetTransactionsByBlocksReq) ([]*model.Transaction, uint64, uint64, error) {
	height, err := s.client.GetHeight(ctx)
	if err != nil {
		return nil, 0, 0, err
	}

	transactions, totalItems, err := s.transactionStore.GetTransactionsBetweenBlocks(ctx, req.Addresses, req.FromBlock, req.ToBlock, req.Limit, req.Offset)
	if err != nil {
		return nil, 0, 0, err
	}

	return transactions, totalItems, height.Height, nil
}

func (s *XTZService) GetTransactionsByDates(ctx context.Context, req *GetTransactionsByDatesReq) ([]*model.Transaction, uint64, uint64, error) {
	transactions, totalItems, err := s.transactionStore.GetTransactionsBetweenDates(ctx, req.Addresses, req.FromDate, req.ToDate, req.Limit, req.Offset)
	if err != nil {
		return nil, 0, 0, err
	}

	height, err := s.client.GetHeight(ctx)
	if err != nil {
		return nil, 0, 0, err
	}

	return transactions, totalItems, height.Height, nil
}

func (s *XTZService) GetRawTransactionHash(ctx context.Context, rawTransaction string) (string, error) {
	return s.client.GetRawTransactionHash(ctx, rawTransaction)
}
