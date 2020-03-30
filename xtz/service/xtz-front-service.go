package service

import (
	"context"
	"time"

	common_service "github.com/t-dx/tg-blocksd/pkg/common/service"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"
)

type AddAddressesReq struct {
	Network   string   `validate:"required,blockchainnetworkmainnet"`
	Addresses []string `validate:"required,lt=100,dive,min=1,max=1000,xtzaddress"`
}

type BroadcastByCustomerReq struct {
	Network        string            `validate:"required,blockchainnetworkmainnet"`
	CustomerID     string            `validate:"required,max=100,safestring"`
	RawTransaction string            `validate:"required,min=1,max=10000,xtzrawtransaction"`
	Attributes     map[string]string `validate:"max=100,dive,keys,max=254,safestring,endkeys,max=254,generalstring"`
}

type GetBlockchainInfoReq struct {
	Network string `validate:"required,blockchainnetworkmainnet"`
}

type GetEstimatedFeeReq struct {
	Network string `validate:"required,blockchainnetworkmainnet"`
}

type GetBalancesReq struct {
	Network     string   `validate:"required,blockchainnetworkmainnet"`
	Addresses   []string `validate:"required,lt=100,dive,min=1,max=1000,xtzaddress"`
	BlockNumber uint64
}

type GetCountersReq struct {
	Network   string   `validate:"required,blockchainnetworkmainnet"`
	Addresses []string `validate:"required,lt=100,dive,min=1,max=1000,xtzaddress"`
}

type GetTransactionsByHashesByCustomerReq struct {
	Network    string   `validate:"required,blockchainnetworkmainnet"`
	CustomerID string   `validate:"required,max=100,safestring"`
	Hashes     []string `validate:"required,lt=100,dive,xtzhash"`
}

type GetTransactionsByBlocksByCustomerReq struct {
	Network    string   `validate:"required,blockchainnetworkmainnet"`
	CustomerID string   `validate:"required,max=100,safestring"`
	Addresses  []string `validate:"required,lt=100,dive,min=1,max=1000,xtzaddress"`
	FromBlock  uint64
	ToBlock    uint64 `validate:"eq=0|gtecsfield=FromBlock"`
	Limit      uint64 `validate:"lt=200"`
	Offset     uint64
}

type GetTransactionsByDatesByCustomerReq struct {
	Network    string   `validate:"required,blockchainnetworkmainnet"`
	CustomerID string   `validate:"required,max=100,safestring"`
	Addresses  []string `validate:"required,lt=100,dive,min=1,max=1000,xtzaddress"`
	FromDate   time.Time
	ToDate     time.Time `validate:"gtecsfield=FromDate"`
	Limit      uint64    `validate:"lt=200"`
	Offset     uint64
}

type GetTransactionsByAttributesByCustomerReq struct {
	Network        string `validate:"required,blockchainnetworkmainnet"`
	CustomerID     string `validate:"required,max=100,safestring"`
	AttributeKey   string `validate:"required,max=254,safestring"`
	AttributeValue string `validate:"required,max=254,generalstring"`
}

// XTZFronter defines the tezos service API.
type XTZFronter interface {
	AddAddresses(ctx context.Context, req *AddAddressesReq) error
	Broadcast(ctx context.Context, req *BroadcastByCustomerReq) (string, error)
	GetBlockchainInfo(ctx context.Context, req *GetBlockchainInfoReq) (*model.BlockchainInfo, error)
	GetEstimatedFee(ctx context.Context, req *GetEstimatedFeeReq) (*model.Fees, error)
	GetBalances(ctx context.Context, req *GetBalancesReq) ([]*model.Balance, error)
	GetCounters(ctx context.Context, req *GetCountersReq) ([]*model.Counter, error)
	GetTransactionsByHashes(ctx context.Context, req *GetTransactionsByHashesByCustomerReq) ([]*model.Transaction, uint64, error)
	GetTransactionsByBlocks(ctx context.Context, req *GetTransactionsByBlocksByCustomerReq) ([]*model.Transaction, uint64, uint64, error)
	GetTransactionsByDates(ctx context.Context, req *GetTransactionsByDatesByCustomerReq) ([]*model.Transaction, uint64, uint64, error)
	GetTransactionsByAttributes(ctx context.Context, req *GetTransactionsByAttributesByCustomerReq) ([]*model.Transaction, uint64, error)
}

// XTZFrontService is the tezos service handler.
type XTZFrontService struct {
	xtzService                     XTZer
	xtzTransactionAttributeService common_service.TransactionAttributer
}

// Verify XTZFrontService satisfies the XTZFronter interface.
var _ XTZFronter = (*XTZFrontService)(nil)

// NewXTZFrontService returns a fresh tezos service instance.
func NewXTZFrontService(xtzService XTZer, xtzTransactionAttributeService common_service.TransactionAttributer) *XTZFrontService {
	return &XTZFrontService{xtzService: xtzService, xtzTransactionAttributeService: xtzTransactionAttributeService}
}

func (s *XTZFrontService) AddAddresses(ctx context.Context, req *AddAddressesReq) error {
	return s.xtzService.AddAddresses(ctx, req)
}

func (s *XTZFrontService) Broadcast(ctx context.Context, req *BroadcastByCustomerReq) (string, error) {
	hash, err := s.xtzService.GetRawTransactionHash(ctx, req.RawTransaction)
	if err != nil {
		return "", err
	}

	if req.Attributes != nil {
		err = s.xtzTransactionAttributeService.CreateTransactionAttributes(ctx, &common_service.CreateTransactionAttributesReq{
			CustomerID: req.CustomerID,
			Hash:       hash,
			Attributes: req.Attributes,
		})
		if err != nil {
			return "", err
		}
	}

	return s.xtzService.Broadcast(ctx, &BroadcastReq{
		Network:        req.Network,
		RawTransaction: req.RawTransaction,
	})
}

func (s *XTZFrontService) GetBlockchainInfo(ctx context.Context, req *GetBlockchainInfoReq) (*model.BlockchainInfo, error) {
	return s.xtzService.GetBlockchainInfo(ctx, req)
}

func (s *XTZFrontService) GetEstimatedFee(ctx context.Context, req *GetEstimatedFeeReq) (*model.Fees, error) {
	return s.xtzService.GetEstimatedFee(ctx, req)
}

func (s *XTZFrontService) GetBalances(ctx context.Context, req *GetBalancesReq) ([]*model.Balance, error) {
	return s.xtzService.GetBalances(ctx, req)
}

func (s *XTZFrontService) GetCounters(ctx context.Context, req *GetCountersReq) ([]*model.Counter, error) {
	return s.xtzService.GetCounters(ctx, req)
}

func (s *XTZFrontService) GetTransactionsByHashes(ctx context.Context, req *GetTransactionsByHashesByCustomerReq) ([]*model.Transaction, uint64, error) {
	transactions, height, err := s.xtzService.GetTransactionsByHashes(ctx, &GetTransactionsByHashesReq{
		Network: req.Network,
		Hashes:  req.Hashes,
	})
	if err != nil {
		return nil, 0, err
	}

	m, err := s.xtzTransactionAttributeService.GetAttributesMap(ctx, &common_service.GetAttributesMapReq{
		CustomerID: req.CustomerID,
		Hashes:     req.Hashes,
	})
	if err != nil {
		return nil, 0, err
	}

	for _, transaction := range transactions {
		attribute, ok := m[transaction.Hash]
		if ok {
			transaction.Attributes = attribute.Attributes
		}
	}

	return transactions, height, nil
}

func (s *XTZFrontService) GetTransactionsByBlocks(ctx context.Context, req *GetTransactionsByBlocksByCustomerReq) ([]*model.Transaction, uint64, uint64, error) {
	transactions, totalItems, height, err := s.xtzService.GetTransactionsByBlocks(ctx, &GetTransactionsByBlocksReq{
		Network:   req.Network,
		Addresses: req.Addresses,
		FromBlock: req.FromBlock,
		ToBlock:   req.ToBlock,
		Limit:     req.Limit,
		Offset:    req.Offset,
	})
	if err != nil {
		return nil, 0, 0, err
	}

	hashes := []string{}
	for _, transaction := range transactions {
		hashes = append(hashes, transaction.Hash)
	}

	m, err := s.xtzTransactionAttributeService.GetAttributesMap(ctx, &common_service.GetAttributesMapReq{
		CustomerID: req.CustomerID,
		Hashes:     hashes,
	})
	if err != nil {
		return nil, 0, 0, err
	}

	for _, transaction := range transactions {
		attribute, ok := m[transaction.Hash]
		if ok {
			transaction.Attributes = attribute.Attributes
		}
	}

	return transactions, totalItems, height, nil
}

func (s *XTZFrontService) GetTransactionsByDates(ctx context.Context, req *GetTransactionsByDatesByCustomerReq) ([]*model.Transaction, uint64, uint64, error) {
	transactions, totalItems, height, err := s.xtzService.GetTransactionsByDates(ctx, &GetTransactionsByDatesReq{
		Network:   req.Network,
		Addresses: req.Addresses,
		FromDate:  req.FromDate,
		ToDate:    req.ToDate,
		Limit:     req.Limit,
		Offset:    req.Offset,
	})
	if err != nil {
		return nil, 0, 0, err
	}

	hashes := []string{}
	for _, transaction := range transactions {
		hashes = append(hashes, transaction.Hash)
	}

	m, err := s.xtzTransactionAttributeService.GetAttributesMap(ctx, &common_service.GetAttributesMapReq{
		CustomerID: req.CustomerID,
		Hashes:     hashes,
	})
	if err != nil {
		return nil, 0, 0, err
	}

	for _, transaction := range transactions {
		attribute, ok := m[transaction.Hash]
		if ok {
			transaction.Attributes = attribute.Attributes
		}
	}

	return transactions, totalItems, height, nil
}

func (s *XTZFrontService) GetTransactionsByAttributes(ctx context.Context, req *GetTransactionsByAttributesByCustomerReq) ([]*model.Transaction, uint64, error) {
	transactionAttributes, err := s.xtzTransactionAttributeService.SearchTransactionAttributes(ctx, &common_service.SearchTransactionAttributesReq{
		CustomerID: req.CustomerID,
		Key:        req.AttributeKey,
		Value:      req.AttributeValue,
	})
	if err != nil {
		return nil, 0, err
	}

	hashes := []string{}
	for _, transactionAttribute := range transactionAttributes {
		hashes = append(hashes, transactionAttribute.TransactionHash)
	}

	return s.GetTransactionsByHashes(ctx, &GetTransactionsByHashesByCustomerReq{
		Network:    req.Network,
		CustomerID: req.CustomerID,
		Hashes:     hashes,
	})
}
