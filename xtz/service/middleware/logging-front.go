package middleware

import (
	"context"
	"time"

	"github.com/t-dx/tg-blocksd/internal/logger"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"
	"github.com/t-dx/tg-blocksd/pkg/xtz/service"

	"go.uber.org/zap"
)

func LoggingFront(logger *logger.ContextLogger) func(service.XTZFronter) service.XTZFronter {
	return func(next service.XTZFronter) service.XTZFronter {
		return &loggingFront{
			logger: logger,
			next:   next,
		}
	}
}

type loggingFront struct {
	logger *logger.ContextLogger
	next   service.XTZFronter
}

func (mw *loggingFront) AddAddresses(ctx context.Context, req *service.AddAddressesReq) error {
	now := time.Now()

	err := mw.next.AddAddresses(ctx, req)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "AddAddresses"),
			zap.Error(err),
			zap.Int("num_addresses", len(req.Addresses)),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return err
	}

	mw.logger.Info(ctx, "request completed",
		zap.String("method", "AddAddresses"),
		zap.Int("num_addresses", len(req.Addresses)),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return nil
}

func (mw *loggingFront) Broadcast(ctx context.Context, req *service.BroadcastByCustomerReq) (string, error) {
	now := time.Now()

	res, err := mw.next.Broadcast(ctx, req)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "Broadcast"),
			zap.Error(err),
			zap.String("customer_id", req.CustomerID),
			zap.String("raw_transaction", req.RawTransaction),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, err
	}

	mw.logger.Info(ctx, "request completed",
		zap.String("method", "Broadcast"),
		zap.String("customer_id", req.CustomerID),
		zap.String("raw_transaction", req.RawTransaction),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, nil
}

func (mw *loggingFront) GetBlockchainInfo(ctx context.Context, req *service.GetBlockchainInfoReq) (*model.BlockchainInfo, error) {
	now := time.Now()

	res, err := mw.next.GetBlockchainInfo(ctx, req)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetBlockchainInfo"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, err
	}

	mw.logger.Info(ctx, "request completed",
		zap.String("method", "GetBlockchainInfo"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, nil
}

func (mw *loggingFront) GetEstimatedFee(ctx context.Context, req *service.GetEstimatedFeeReq) (*model.Fees, error) {
	now := time.Now()

	res, err := mw.next.GetEstimatedFee(ctx, req)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetEstimatedFee"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, err
	}

	mw.logger.Info(ctx, "request completed",
		zap.String("method", "GetEstimatedFee"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, nil
}

func (mw *loggingFront) GetBalances(ctx context.Context, req *service.GetBalancesReq) ([]*model.Balance, error) {
	now := time.Now()

	res, err := mw.next.GetBalances(ctx, req)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetBalances"),
			zap.Error(err),
			zap.Int("num_addresses", len(req.Addresses)),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, err
	}

	mw.logger.Info(ctx, "request completed",
		zap.String("method", "GetBalances"),
		zap.Int("num_addresses", len(req.Addresses)),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, nil
}

func (mw *loggingFront) GetCounters(ctx context.Context, req *service.GetCountersReq) ([]*model.Counter, error) {
	now := time.Now()

	res, err := mw.next.GetCounters(ctx, req)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetCounters"),
			zap.Error(err),
			zap.Int("num_addresses", len(req.Addresses)),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, err
	}

	mw.logger.Info(ctx, "request completed",
		zap.String("method", "GetCounters"),
		zap.Int("num_addresses", len(req.Addresses)),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, nil
}

func (mw *loggingFront) GetTransactionsByHashes(ctx context.Context, req *service.GetTransactionsByHashesByCustomerReq) ([]*model.Transaction, uint64, error) {
	now := time.Now()

	res, height, err := mw.next.GetTransactionsByHashes(ctx, req)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetTransactionsByHashes"),
			zap.Error(err),
			zap.Int("num_hashes", len(req.Hashes)),
			zap.String("customer_id", req.CustomerID),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, height, err
	}

	mw.logger.Info(ctx, "request completed",
		zap.String("method", "GetTransactionsByHashes"),
		zap.String("customer_id", req.CustomerID),
		zap.Int("num_hashes", len(req.Hashes)),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, height, nil
}

func (mw *loggingFront) GetTransactionsByBlocks(ctx context.Context, req *service.GetTransactionsByBlocksByCustomerReq) ([]*model.Transaction, uint64, uint64, error) {
	now := time.Now()

	res, totalItems, height, err := mw.next.GetTransactionsByBlocks(ctx, req)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetTransactionsByBlocks"),
			zap.Error(err),
			zap.String("customer_id", req.CustomerID),
			zap.Int("num_addresses", len(req.Addresses)),
			zap.Uint64("from_block", req.FromBlock),
			zap.Uint64("to_block", req.ToBlock),
			zap.Uint64("limit", req.Limit),
			zap.Uint64("offset", req.Offset),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, totalItems, height, err
	}

	mw.logger.Info(ctx, "request completed",
		zap.String("method", "GetTransactionsByBlocks"),
		zap.String("customer_id", req.CustomerID),
		zap.Int("num_transactions", len(res)),
		zap.Uint64("total_items", totalItems),
		zap.Uint64("height", height),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, totalItems, height, nil
}

func (mw *loggingFront) GetTransactionsByDates(ctx context.Context, req *service.GetTransactionsByDatesByCustomerReq) ([]*model.Transaction, uint64, uint64, error) {
	now := time.Now()

	res, totalItems, height, err := mw.next.GetTransactionsByDates(ctx, req)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetTransactionsByDates"),
			zap.Error(err),
			zap.String("customer_id", req.CustomerID),
			zap.Int("num_addresses", len(req.Addresses)),
			zap.Time("from_date", req.FromDate),
			zap.Time("to_date", req.ToDate),
			zap.Uint64("limit", req.Limit),
			zap.Uint64("offset", req.Offset),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, totalItems, height, err
	}

	mw.logger.Info(ctx, "request completed",
		zap.String("method", "GetTransactionsByDates"),
		zap.String("customer_id", req.CustomerID),
		zap.Int("num_transactions", len(res)),
		zap.Uint64("total_items", totalItems),
		zap.Uint64("height", height),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, totalItems, height, nil
}

func (mw *loggingFront) GetTransactionsByAttributes(ctx context.Context, req *service.GetTransactionsByAttributesByCustomerReq) ([]*model.Transaction, uint64, error) {
	now := time.Now()

	res, height, err := mw.next.GetTransactionsByAttributes(ctx, req)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetTransactionsByAttributes"),
			zap.Error(err),
			zap.String("customer_id", req.CustomerID),
			zap.String("attribute_key", req.AttributeKey),
			zap.String("attribute_value", req.AttributeValue),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, height, err
	}

	mw.logger.Info(ctx, "request completed",
		zap.String("method", "GetTransactionsByAttributes"),
		zap.String("customer_id", req.CustomerID),
		zap.Int("num_transactions", len(res)),
		zap.Uint64("height", height),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, height, nil
}
