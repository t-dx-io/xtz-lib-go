package middleware

import (
	"context"
	"sort"
	"time"

	"github.com/t-dx/tg-blocksd/internal/logger"
	"github.com/t-dx/tg-blocksd/internal/utils/cache"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"
	"github.com/t-dx/tg-blocksd/pkg/xtz/service"

	"github.com/coocood/freecache"
	"go.uber.org/zap"
)

const (
	cacheSize = 500 * 1024 * 1024 // Max 500MB stored in memory

	// Caches expiration in seconds
	addAddressesCacheExpiration                = 60
	broadcastCacheExpiration                   = 60
	getBlockchainInfoCacheExpiration           = 15
	getEstimatedFeeCacheExpiration             = 60
	getBalancesCacheExpiration                 = 15
	getCountersCacheExpiration                 = 15
	getTransactionsByHashesCacheExpiration     = 60
	getTransactionsByBlocksCacheExpiration     = 60
	getTransactionsByDatesCacheExpiration      = 60
	getTransactionsByAttributesCacheExpiration = 60
	CallContractMethodCacheExpiration          = 60
)

func Caching() func(service.XTZer) service.XTZer {
	return func(next service.XTZer) service.XTZer {
		return &caching{
			cache: freecache.NewCache(cacheSize),
			next:  next,
		}
	}
}

type caching struct {
	cache *freecache.Cache
	next  service.XTZer
}

func (mw *caching) AddAddresses(ctx context.Context, req *service.AddAddressesReq) error {
	return mw.next.AddAddresses(ctx, req)
}

func (mw *caching) Broadcast(ctx context.Context, req *service.BroadcastReq) (string, error) {
	key, err := cache.GenKey("Broadcast", req)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.Broadcast(ctx, req)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var hash string
		if err := cache.Decode(cached, &hash); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return hash, nil
		}
	}

	// Cache miss: use client to get result.
	hash, err := mw.next.Broadcast(ctx, req)
	if err != nil {
		return "", err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(hash); err == nil {
		err = mw.cache.Set(key, toCache, broadcastCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return hash, nil
}

func (mw *caching) GetBlockchainInfo(ctx context.Context, req *service.GetBlockchainInfoReq) (*model.BlockchainInfo, error) {
	return mw.next.GetBlockchainInfo(ctx, req)
}

func (mw *caching) GetEstimatedFee(ctx context.Context, req *service.GetEstimatedFeeReq) (*model.Fees, error) {
	return mw.next.GetEstimatedFee(ctx, req)
}

func (mw *caching) GetBalances(ctx context.Context, req *service.GetBalancesReq) ([]*model.Balance, error) {
	return mw.next.GetBalances(ctx, req)
}

func (mw *caching) GetCounters(ctx context.Context, req *service.GetCountersReq) ([]*model.Counter, error) {
	return mw.next.GetCounters(ctx, req)
}

func (mw *caching) GetTransactionsByHashes(ctx context.Context, req *service.GetTransactionsByHashesReq) ([]*model.Transaction, uint64, error) {
	sort.Strings(req.Hashes)
	key, err := cache.GenKey("GetTransactionsByHashes", req)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.GetTransactionsByHashes(ctx, req)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var cachedTransactions cachedTransactions
		if err := cache.Decode(cached, &cachedTransactions); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return cachedTransactions.Transactions, cachedTransactions.Height, nil
		}
	}

	// Cache miss: use client to get result.
	transactions, height, err := mw.next.GetTransactionsByHashes(ctx, req)
	if err != nil {
		return nil, 0, err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(cachedTransactions{Transactions: transactions, Height: height}); err == nil {
		err = mw.cache.Set(key, toCache, getTransactionsByHashesCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return transactions, height, nil
}

func (mw *caching) GetTransactionsByBlocks(ctx context.Context, req *service.GetTransactionsByBlocksReq) ([]*model.Transaction, uint64, uint64, error) {
	sort.Strings(req.Addresses)
	key, err := cache.GenKey("GetTransactionsByBlocks", req)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.GetTransactionsByBlocks(ctx, req)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var cachedTransactions cachedTransactions
		if err := cache.Decode(cached, &cachedTransactions); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return cachedTransactions.Transactions, cachedTransactions.TotalItems, cachedTransactions.Height, nil
		}
	}

	// Cache miss: use client to get result.
	transactions, totalItems, height, err := mw.next.GetTransactionsByBlocks(ctx, req)
	if err != nil {
		return nil, 0, 0, err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(cachedTransactions{Transactions: transactions, TotalItems: totalItems, Height: height}); err == nil {
		err = mw.cache.Set(key, toCache, getTransactionsByBlocksCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return transactions, totalItems, height, nil
}

func (mw *caching) GetTransactionsByDates(ctx context.Context, req *service.GetTransactionsByDatesReq) ([]*model.Transaction, uint64, uint64, error) {
	sort.Strings(req.Addresses)
	req.FromDate = req.FromDate.Truncate(time.Minute)
	req.ToDate = req.ToDate.Truncate(time.Minute)
	key, err := cache.GenKey("GetTransactionsByDates", req)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.GetTransactionsByDates(ctx, req)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var cachedTransactions cachedTransactions
		if err := cache.Decode(cached, &cachedTransactions); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return cachedTransactions.Transactions, cachedTransactions.TotalItems, cachedTransactions.Height, nil
		}
	}

	// Cache miss: use client to get result.
	transactions, totalItems, height, err := mw.next.GetTransactionsByDates(ctx, req)
	if err != nil {
		return nil, 0, 0, err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(cachedTransactions{Transactions: transactions, TotalItems: totalItems, Height: height}); err == nil {
		err = mw.cache.Set(key, toCache, getTransactionsByDatesCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return transactions, totalItems, height, nil
}

func (mw *caching) GetRawTransactionHash(ctx context.Context, rawTransaction string) (string, error) {
	return mw.next.GetRawTransactionHash(ctx, rawTransaction)
}
