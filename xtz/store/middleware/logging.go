package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/t-dx/tg-blocksd/internal/logger"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"
	"github.com/t-dx/tg-blocksd/pkg/xtz/service"

	"go.uber.org/zap"
)

func Logging(logger *logger.ContextLogger) func(service.TransactionStore) service.TransactionStore {
	return func(next service.TransactionStore) service.TransactionStore {
		return &storageLogging{
			logger: logger,
			next:   next,
		}
	}
}

type storageLogging struct {
	logger *logger.ContextLogger
	next   service.TransactionStore
}

func (mw *storageLogging) CreateTransactions(ctx context.Context, transactions []*model.Transaction) error {
	result := []string{}
	for _, transaction := range transactions {
		result = append(result, fmt.Sprintf("%+v", transaction))
	}
	mw.logger.Debug(ctx, "request started", zap.String("method", "CreateTransactions"), zap.Strings("transactions", result))

	now := time.Now()

	err := mw.next.CreateTransactions(ctx, transactions)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "CreateTransactions"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "CreateTransactions"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return nil
}

func (mw *storageLogging) GetTransactions(ctx context.Context, hashes []string) ([]*model.Transaction, error) {
	mw.logger.Debug(ctx, "request started", zap.String("method", "GetTransactions"), zap.Strings("hashes", hashes))

	now := time.Now()

	res, err := mw.next.GetTransactions(ctx, hashes)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetTransactions"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "GetTransactions"),
		zap.String("result", fmt.Sprintf("%+v", res)),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, nil
}

func (mw *storageLogging) GetTransactionsBetweenBlocks(ctx context.Context, addresses []string, fromBlock, toBlock uint64, limit, offset uint64) ([]*model.Transaction, uint64, error) {
	mw.logger.Debug(ctx, "request started", zap.String("method", "GetTransactionsBetweenBlocks"), zap.Strings("addresses", addresses), zap.Uint64("from_block", fromBlock), zap.Uint64("to_block", toBlock), zap.Uint64("limit", limit), zap.Uint64("offset", offset))

	now := time.Now()

	res, totalItems, err := mw.next.GetTransactionsBetweenBlocks(ctx, addresses, fromBlock, toBlock, limit, offset)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetTransactionsBetweenBlocks"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, totalItems, err
	}

	result := []string{}
	for _, r := range res {
		result = append(result, fmt.Sprintf("%+v", r))
	}
	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "GetTransactionsBetweenBlocks"),
		zap.Strings("result", result),
		zap.Uint64("total_items", totalItems),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, totalItems, nil
}

func (mw *storageLogging) GetTransactionsBetweenDates(ctx context.Context, addresses []string, fromDate, toDate time.Time, limit, offset uint64) ([]*model.Transaction, uint64, error) {
	mw.logger.Debug(ctx, "request started", zap.String("method", "GetTransactionsBetweenDates"), zap.Strings("addresses", addresses), zap.Time("from_date", fromDate), zap.Time("to_date", toDate), zap.Uint64("limit", limit), zap.Uint64("offset", offset))

	now := time.Now()

	res, totalItems, err := mw.next.GetTransactionsBetweenDates(ctx, addresses, fromDate, toDate, limit, offset)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetTransactionsBetweenDates"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, totalItems, err
	}

	result := []string{}
	for _, r := range res {
		result = append(result, fmt.Sprintf("%+v", r))
	}
	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "GetTransactionsBetweenDates"),
		zap.Strings("result", result),
		zap.Uint64("total_items", totalItems),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, totalItems, nil
}

func (mw *storageLogging) MarkPinned(ctx context.Context, addresses []string) error {
	mw.logger.Debug(ctx, "request started", zap.String("method", "MarkPinned"), zap.Strings("addresses", addresses))

	now := time.Now()

	err := mw.next.MarkPinned(ctx, addresses)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "MarkPinned"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "MarkPinned"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return nil
}

func (mw *storageLogging) Broadcast(ctx context.Context, transaction *model.Transaction) error {
	mw.logger.Debug(ctx, "request started", zap.String("method", "Broadcast"), zap.String("transaction", fmt.Sprintf("%+v", transaction)))

	now := time.Now()

	err := mw.next.Broadcast(ctx, transaction)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "Broadcast"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "Broadcast"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return nil
}

func (mw *storageLogging) GetPendingBroadcasts(ctx context.Context, broadcastedBeforeBlock, limit uint64) ([]*model.Transaction, error) {
	mw.logger.Debug(ctx, "request started", zap.String("method", "GetPendingBroadcasts"), zap.Uint64("before_block", broadcastedBeforeBlock), zap.Uint64("limit", limit))

	now := time.Now()

	res, err := mw.next.GetPendingBroadcasts(ctx, broadcastedBeforeBlock, limit)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetPendingBroadcasts"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return res, err
	}

	result := []string{}
	for _, r := range res {
		result = append(result, fmt.Sprintf("%+v", r))
	}
	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "GetPendingBroadcasts"),
		zap.Strings("result", result),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, nil
}

func (mw *storageLogging) UpdateBroadcast(ctx context.Context, hash, status, message string, broadcastedAtBlock uint64) error {
	mw.logger.Debug(ctx, "request started", zap.String("method", "UpdateBroadcast"), zap.String("hash", hash), zap.String("status", status), zap.String("message", message), zap.Uint64("broadcasted_at_block", broadcastedAtBlock))

	now := time.Now()

	err := mw.next.UpdateBroadcast(ctx, hash, status, message, broadcastedAtBlock)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "UpdateBroadcast"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "UpdateBroadcast"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return nil
}

func (mw *storageLogging) GetBroadcastsToGarbageCollect(ctx context.Context, beforeBlock uint64) ([]string, error) {
	mw.logger.Debug(ctx, "request started", zap.String("method", "GetBroadcastsToGarbageCollect"), zap.Uint64("before_block", beforeBlock))

	now := time.Now()

	res, err := mw.next.GetBroadcastsToGarbageCollect(ctx, beforeBlock)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GetBroadcastsToGarbageCollect"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return nil, err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "GetBroadcastsToGarbageCollect"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return res, nil
}

func (mw *storageLogging) GarbageCollectBroadcasts(ctx context.Context, broadcastHashes []string) error {
	mw.logger.Debug(ctx, "request started", zap.String("method", "GarbageCollectBroadcasts"))

	now := time.Now()

	err := mw.next.GarbageCollectBroadcasts(ctx, broadcastHashes)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GarbageCollectBroadcasts"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "GarbageCollectBroadcasts"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return nil
}

func (mw *storageLogging) GarbageCollectTransactions(ctx context.Context, beforeBlock uint64) error {
	mw.logger.Debug(ctx, "request started", zap.String("method", "GarbageCollectTransactions"), zap.Uint64("before_block", beforeBlock))

	now := time.Now()

	err := mw.next.GarbageCollectTransactions(ctx, beforeBlock)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "GarbageCollectTransactions"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "GarbageCollectTransactions"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return nil
}

func (mw *storageLogging) DumpPendingBroadcasts(ctx context.Context, limit, offset uint64, asOfSystemTime time.Time) ([]*model.Transaction, uint64, error) {
	mw.logger.Debug(ctx, "request started", zap.String("method", "DumpPendingBroadcasts"), zap.Uint64("limit", limit), zap.Uint64("offset", offset), zap.Time("system_time", asOfSystemTime))

	now := time.Now()

	transactions, count, err := mw.next.DumpPendingBroadcasts(ctx, limit, offset, asOfSystemTime)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "DumpPendingBroadcasts"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return transactions, count, err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "DumpPendingBroadcasts"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return transactions, count, err
}

func (mw *storageLogging) DumpPinnedTransactions(ctx context.Context, limit, offset uint64, asOfSystemTime time.Time) ([]*model.Transaction, uint64, error) {
	mw.logger.Debug(ctx, "request started", zap.String("method", "DumpPinnedTransactions"), zap.Uint64("limit", limit), zap.Uint64("offset", offset), zap.Time("system_time", asOfSystemTime))

	now := time.Now()

	transactions, count, err := mw.next.DumpPinnedTransactions(ctx, limit, offset, asOfSystemTime)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "DumpPinnedTransactions"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return transactions, count, err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "DumpPinnedTransactions"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return transactions, count, err
}

func (mw *storageLogging) DeleteBlockTransactions(ctx context.Context, blockNumber uint64) error {
	mw.logger.Debug(ctx, "request started", zap.String("method", "DeleteBlockTransactions"), zap.Uint64("blockNumber", blockNumber))

	now := time.Now()

	err := mw.next.DeleteBlockTransactions(ctx, blockNumber)
	if err != nil {
		mw.logger.Error(ctx, "request failed",
			zap.String("method", "DeleteBlockTransactions"),
			zap.Error(err),
			zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
		)
		return err
	}

	mw.logger.Debug(ctx, "request completed",
		zap.String("method", "DeleteBlockTransactions"),
		zap.Float64("elapsed_ms", float64(time.Since(now).Nanoseconds())/1000000.0),
	)
	return err
}
