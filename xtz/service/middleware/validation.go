package middleware

import (
	"context"

	"github.com/t-dx/tg-blocksd/pkg/xtz/model"
	"github.com/t-dx/tg-blocksd/pkg/xtz/service"

	val "github.com/go-playground/validator/v10"
)

func Validation(validate *val.Validate) func(service.XTZFronter) service.XTZFronter {
	return func(next service.XTZFronter) service.XTZFronter {
		return &validation{next: next, validate: validate}
	}
}

type validation struct {
	next     service.XTZFronter
	validate *val.Validate
}

func (mw *validation) AddAddresses(ctx context.Context, req *service.AddAddressesReq) error {
	err := mw.validate.Struct(req)
	if err != nil {
		return err
	}
	return mw.next.AddAddresses(ctx, req)
}

func (mw *validation) Broadcast(ctx context.Context, req *service.BroadcastByCustomerReq) (string, error) {
	err := mw.validate.Struct(req)
	if err != nil {
		return "", err
	}
	return mw.next.Broadcast(ctx, req)
}

func (mw *validation) GetBlockchainInfo(ctx context.Context, req *service.GetBlockchainInfoReq) (*model.BlockchainInfo, error) {
	err := mw.validate.Struct(req)
	if err != nil {
		return nil, err
	}
	return mw.next.GetBlockchainInfo(ctx, req)
}

func (mw *validation) GetEstimatedFee(ctx context.Context, req *service.GetEstimatedFeeReq) (*model.Fees, error) {
	err := mw.validate.Struct(req)
	if err != nil {
		return nil, err
	}
	return mw.next.GetEstimatedFee(ctx, req)
}

func (mw *validation) GetBalances(ctx context.Context, req *service.GetBalancesReq) ([]*model.Balance, error) {
	err := mw.validate.Struct(req)
	if err != nil {
		return nil, err
	}

	return mw.next.GetBalances(ctx, req)
}

func (mw *validation) GetCounters(ctx context.Context, req *service.GetCountersReq) ([]*model.Counter, error) {
	err := mw.validate.Struct(req)
	if err != nil {
		return nil, err
	}
	return mw.next.GetCounters(ctx, req)
}

func (mw *validation) GetTransactionsByHashes(ctx context.Context, req *service.GetTransactionsByHashesByCustomerReq) ([]*model.Transaction, uint64, error) {
	err := mw.validate.Struct(req)
	if err != nil {
		return nil, 0, err
	}
	return mw.next.GetTransactionsByHashes(ctx, req)
}

func (mw *validation) GetTransactionsByBlocks(ctx context.Context, req *service.GetTransactionsByBlocksByCustomerReq) ([]*model.Transaction, uint64, uint64, error) {
	err := mw.validate.Struct(req)
	if err != nil {
		return nil, 0, 0, err
	}
	return mw.next.GetTransactionsByBlocks(ctx, req)
}

func (mw *validation) GetTransactionsByDates(ctx context.Context, req *service.GetTransactionsByDatesByCustomerReq) ([]*model.Transaction, uint64, uint64, error) {
	err := mw.validate.Struct(req)
	if err != nil {
		return nil, 0, 0, err
	}
	return mw.next.GetTransactionsByDates(ctx, req)
}

func (mw *validation) GetTransactionsByAttributes(ctx context.Context, req *service.GetTransactionsByAttributesByCustomerReq) ([]*model.Transaction, uint64, error) {
	err := mw.validate.Struct(req)
	if err != nil {
		return nil, 0, err
	}
	return mw.next.GetTransactionsByAttributes(ctx, req)
}
