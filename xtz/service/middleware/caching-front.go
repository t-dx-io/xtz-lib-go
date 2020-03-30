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

func CachingFront() func(service.XTZFronter) service.XTZFronter {
	return func(next service.XTZFronter) service.XTZFronter {
		return &cachingFront{
			cache: freecache.NewCache(cacheSize),
			next:  next,
		}
	}
}

type cachingFront struct {
	cache *freecache.Cache
	next  service.XTZFronter
}

func (mw *cachingFront) AddAddresses(ctx context.Context, req *service.AddAddressesReq) error {
	sort.Strings(req.Addresses)
	key, err := cache.GenKey("AddAddresses", req)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.AddAddresses(ctx, req)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var cachedError error
		if err := cache.Decode(cached, &cachedError); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return cachedError
		}
	}

	// Cache miss: use client to get result.
	errToCache := mw.next.AddAddresses(ctx, req)
	if errToCache != nil {
		return errToCache
	}

	// Store result in cache.
	if toCache, err := cache.Encode(errToCache); err == nil {
		err = mw.cache.Set(key, toCache, addAddressesCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return errToCache
}

func (mw *cachingFront) Broadcast(ctx context.Context, req *service.BroadcastByCustomerReq) (string, error) {
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

func (mw *cachingFront) GetBlockchainInfo(ctx context.Context, req *service.GetBlockchainInfoReq) (*model.BlockchainInfo, error) {
	key, err := cache.GenKey("GetBlockchainInfo", req)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.GetBlockchainInfo(ctx, req)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var info *model.BlockchainInfo
		if err := cache.Decode(cached, &info); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return info, nil
		}
	}

	// Cache miss: use client to get result.
	info, err := mw.next.GetBlockchainInfo(ctx, req)
	if err != nil {
		return nil, err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(info); err == nil {
		err = mw.cache.Set(key, toCache, getBlockchainInfoCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return info, nil
}

func (mw *cachingFront) GetEstimatedFee(ctx context.Context, req *service.GetEstimatedFeeReq) (*model.Fees, error) {
	key, err := cache.GenKey("GetEstimatedFee", req)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.GetEstimatedFee(ctx, req)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var fee *model.Fees
		if err := cache.Decode(cached, &fee); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return fee, nil
		}
	}

	// Cache miss: use client to get result.
	fee, err := mw.next.GetEstimatedFee(ctx, req)
	if err != nil {
		return nil, err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(fee); err == nil {
		err = mw.cache.Set(key, toCache, getEstimatedFeeCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return fee, nil
}

func (mw *cachingFront) GetBalances(ctx context.Context, req *service.GetBalancesReq) ([]*model.Balance, error) {
	sort.Strings(req.Addresses)
	key, err := cache.GenKey("GetBalances", req)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.GetBalances(ctx, req)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var balances []*model.Balance
		if err := cache.Decode(cached, &balances); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return balances, nil
		}
	}

	// Cache miss: use client to get result.
	balances, err := mw.next.GetBalances(ctx, req)
	if err != nil {
		return nil, err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(balances); err == nil {
		err = mw.cache.Set(key, toCache, getBalancesCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return balances, nil
}

func (mw *cachingFront) GetCounters(ctx context.Context, req *service.GetCountersReq) ([]*model.Counter, error) {
	sort.Strings(req.Addresses)
	key, err := cache.GenKey("GetCounters", req)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.GetCounters(ctx, req)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var counters []*model.Counter
		if err := cache.Decode(cached, &counters); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return counters, nil
		}
	}

	// Cache miss: use client to get result.
	counters, err := mw.next.GetCounters(ctx, req)
	if err != nil {
		return nil, err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(counters); err == nil {
		err = mw.cache.Set(key, toCache, getCountersCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return counters, nil
}

type cachedTransactions struct {
	Transactions []*model.Transaction
	TotalItems   uint64
	Height       uint64
}

func (mw *cachingFront) GetTransactionsByHashes(ctx context.Context, req *service.GetTransactionsByHashesByCustomerReq) ([]*model.Transaction, uint64, error) {
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

func (mw *cachingFront) GetTransactionsByBlocks(ctx context.Context, req *service.GetTransactionsByBlocksByCustomerReq) ([]*model.Transaction, uint64, uint64, error) {
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

func (mw *cachingFront) GetTransactionsByDates(ctx context.Context, req *service.GetTransactionsByDatesByCustomerReq) ([]*model.Transaction, uint64, uint64, error) {
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

func (mw *cachingFront) GetTransactionsByAttributes(ctx context.Context, req *service.GetTransactionsByAttributesByCustomerReq) ([]*model.Transaction, uint64, error) {
	key, err := cache.GenKey("GetTransactionsByAttributes", req)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.GetTransactionsByAttributes(ctx, req)
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
	transactions, height, err := mw.next.GetTransactionsByAttributes(ctx, req)
	if err != nil {
		return nil, 0, err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(cachedTransactions{Transactions: transactions, Height: height}); err == nil {
		err = mw.cache.Set(key, toCache, getTransactionsByAttributesCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return transactions, height, nil
}
