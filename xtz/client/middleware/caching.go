package middleware

import (
	"context"

	"github.com/t-dx/tg-blocksd/internal/logger"
	"github.com/t-dx/tg-blocksd/internal/utils/cache"
	common_model "github.com/t-dx/tg-blocksd/pkg/common/model"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"
	"github.com/t-dx/tg-blocksd/pkg/xtz/service"

	"github.com/coocood/freecache"
	"go.uber.org/zap"
)

const (
	cacheSize = 10 * 1024 * 1024 // Max 10MB stored in memory

	// Caches expiration in seconds
	getBlockCacheExpiration  = 60
	getHeightCacheExpiration = 30
)

func Caching() func(service.Client) service.Client {
	return func(next service.Client) service.Client {
		cache := freecache.NewCache(cacheSize)
		return &caching{
			cache: cache,
			next:  next,
		}
	}
}

type caching struct {
	cache *freecache.Cache
	next  service.Client
}

func (mw *caching) BroadcastTransaction(ctx context.Context, rawTransaction string) error {
	return mw.next.BroadcastTransaction(ctx, rawTransaction)
}

func (mw *caching) GetEstimatedFee(ctx context.Context) (*model.Fees, error) {
	return mw.next.GetEstimatedFee(ctx)
}

func (mw *caching) GetBalances(ctx context.Context, addresses []string, blockNumber uint64) ([]*model.Balance, error) {
	return mw.next.GetBalances(ctx, addresses, blockNumber)
}

func (mw *caching) GetBlock(ctx context.Context, blockNumber uint64) (*common_model.Block, error) {
	key, err := cache.GenKey("GetBlock", blockNumber)
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.GetBlock(ctx, blockNumber)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var block *common_model.Block
		if err := cache.Decode(cached, &block); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return block, nil
		}
	}

	// Cache miss: use client to get result.
	block, err := mw.next.GetBlock(ctx, blockNumber)
	if err != nil {
		return nil, err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(block); err == nil {
		err = mw.cache.Set(key, toCache, getBlockCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return block, nil
}

func (mw *caching) GetHeight(ctx context.Context) (*model.Height, error) {
	key, err := cache.GenKey("GetHeight")
	if err != nil {
		logger.TechLog.Error(ctx, "cache key generation error", zap.Error(err))
		return mw.next.GetHeight(ctx)
	}

	// Try to get result from cache.
	if cached, err := mw.cache.Get(key); err == nil {
		var height *model.Height
		if err := cache.Decode(cached, &height); err == nil {
			logger.TechLog.Debug(ctx, "cache hit")
			return height, nil
		}
	}

	// Cache miss: use client to get result.
	height, err := mw.next.GetHeight(ctx)
	if err != nil {
		return nil, err
	}

	// Store result in cache.
	if toCache, err := cache.Encode(height); err == nil {
		err = mw.cache.Set(key, toCache, getHeightCacheExpiration)
		if err != nil {
			logger.TechLog.Error(ctx, "cache error", zap.Error(err))
		}
	}
	logger.TechLog.Debug(ctx, "cache miss")

	return height, nil
}

func (mw *caching) GetCounters(ctx context.Context, addresses []string) ([]*model.Counter, error) {
	return mw.next.GetCounters(ctx, addresses)
}

func (mw *caching) GetRawTransactionHash(ctx context.Context, rawTransaction string) (string, error) {
	return mw.next.GetRawTransactionHash(ctx, rawTransaction)
}

func (mw *caching) GetTransactions(ctx context.Context, blockNumber uint64) ([]*model.Transaction, error) {
	return mw.next.GetTransactions(ctx, blockNumber)
}
