package job

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	job "github.com/t-dx/go-jobs/v4"
	"github.com/t-dx/tg-blocksd/internal/logger"
	"github.com/t-dx/tg-blocksd/pkg/common/service"
	"github.com/t-dx/tg-blocksd/pkg/common/store/cockroach"
	"github.com/t-dx/tg-blocksd/pkg/helper"
	xtz_model "github.com/t-dx/tg-blocksd/pkg/xtz/model"
	xtz_service "github.com/t-dx/tg-blocksd/pkg/xtz/service"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// MetricStore is the interface of the metric store.
type MetricStore interface {
	Put(ctx context.Context, module, key string, metric json.RawMessage) error
}

type BlockFetcher struct {
	BlockStore       service.BlockStore
	TransactionStore xtz_service.TransactionStore
	Client           xtz_service.Client

	BatchSize     int
	ParallelBatch int
	MaxOffset     uint64
	StartBlock    uint64

	MetricStore                 MetricStore
	MetricsBlockIndexed         *prometheus.GaugeVec
	MetricsTransactionsInserted *prometheus.CounterVec
	MetricsBlocksFetched        *prometheus.CounterVec
	MetricsJobDuration          *prometheus.SummaryVec
}

func (bf *BlockFetcher) Do(ctx context.Context, meta job.JobMeta, arg interface{}) (_ interface{}, _ map[string]string, err error) {
	log := logger.With(logger.TechLog, zap.String("job_name", meta.JobName), zap.String("job_id", meta.JobID))

	// Duration metrics
	defer func(begin time.Time) {
		status := "success"
		if err != nil {
			status = "failed"
		}
		bf.MetricsJobDuration.With(helper.MakePrometheusLabels("name", meta.JobName, "status", status)).Observe(time.Since(begin).Seconds())
	}(time.Now())

	log.Info(ctx, "job started", zap.Time("now", time.Now().UTC()))

	// Get the head of the blockchain to compute the headBlock (head - maxOffset).
	height, err := bf.Client.GetHeight(ctx)
	if err != nil {
		log.Error(ctx, "could not get block count", zap.Error(err))
		return nil, map[string]string{"msg": "could not get block count", "error": err.Error()}, err
	}
	headBlock := height.Height - bf.MaxOffset

	log.Info(ctx, "successfully got headBlock", zap.Uint64("head_block", headBlock))

	currentBlock, err := bf.BlockStore.GetLastBlock(ctx)
	var nextBlock uint64
	switch {
	case err == cockroach.ErrNoBlock:
		nextBlock = bf.StartBlock
	case err != nil:
		log.Error(ctx, "could not get next block", zap.Error(err))
		return nil, map[string]string{"msg": "could not get next block", "error": err.Error()}, err
	default:
		nextBlock = currentBlock.Number + 1
	}

	log.Info(ctx, "successfully got nextBlock", zap.Uint64("next_block", nextBlock))

	var processedBlock uint64
	// Put entries in blockstore for the block we have to process.
	for processedBlock = nextBlock; processedBlock <= headBlock; processedBlock++ {
		log.Info(ctx, "start fetching blocks", zap.Uint64("start", nextBlock), zap.Uint64("end", headBlock), zap.Uint64("current", processedBlock))

		block, err := bf.Client.GetBlock(ctx, processedBlock)
		if err != nil {
			log.Error(ctx, "could not get block", zap.Error(err))
			return nil, map[string]string{"msg": "could not get block", "error": err.Error()}, err
		}
		log.Info(ctx, "fetch block", zap.Uint64("block_number", processedBlock))

		// Check for reorgs.
		isReorg, reorgsFromBlockNumber := bf.reorgs(ctx, processedBlock, log)
		if isReorg {
			log.Info(ctx, "reorg", zap.Uint64("current_block", processedBlock), zap.Uint64("reorg_block", reorgsFromBlockNumber))
			processedBlock = reorgsFromBlockNumber
			continue
		}

		transactions, err := bf.Client.GetTransactions(ctx, processedBlock)
		if err != nil {
			log.Error(ctx, "could not get transactions", zap.Error(err))
			return nil, map[string]string{"msg": "could not get transactions", "error": err.Error()}, err
		}

		err = bf.storeTransactions(ctx, transactions, log)
		if err != nil {
			log.Error(ctx, "could not store transactions", zap.Error(err))
			return nil, map[string]string{"msg": "could not store transactions", "error": err.Error()}, err
		}

		// Create block entry, it means the block is finish processing.
		err = bf.BlockStore.CreateBlock(ctx, block)
		if err != nil {
			log.Error(ctx, "could not create block", zap.Uint64("block_number", processedBlock), zap.Error(err))
			return nil, map[string]string{"msg": "could not create block", "error": err.Error()}, err
		}

		// Update metric.
		bf.MetricsBlocksFetched.With(helper.MakePrometheusLabels("coin", "XTZ")).Add(1)
		bf.MetricsTransactionsInserted.With(helper.MakePrometheusLabels("coin", "XTZ")).Add(float64(len(transactions)))
		bf.MetricsBlockIndexed.With(helper.MakePrometheusLabels("coin", "XTZ")).Set(float64(processedBlock))

		jsonMetric := json.RawMessage(fmt.Sprintf(`{"block_number":%d}`, processedBlock))
		err = bf.MetricStore.Put(ctx, "indexer", "XTZ", jsonMetric)
		if err != nil {
			log.Error(ctx, "could not store metric", zap.Uint64("block_number", processedBlock), zap.Error(err))
			return nil, map[string]string{"msg": "could not store metric", "error": err.Error()}, err
		}

		log.Info(ctx, "finished with block", zap.Uint64("block_number", processedBlock))

		err = meta.Update(ctx, map[string]string{"msg": fmt.Sprintf("finished with block %d", processedBlock)})
		if err != nil {
			log.Info(ctx, "could not update job status", zap.Error(err))
		}
	}
	log.Info(ctx, "successfully finished")

	return nil, map[string]string{"msg": fmt.Sprintf("finished with blocks up to %d", processedBlock)}, nil
}

func (bf *BlockFetcher) storeTransactions(ctx context.Context, batch []*xtz_model.Transaction, log *logger.ContextLogger) error {
	errc := make(chan error)

	f := func(txs []*xtz_model.Transaction, idx int) {
		log.Debug(ctx, "inserting transactions batch", zap.Int("from", idx), zap.Int("to", idx+len(txs)))
		var err = bf.TransactionStore.CreateTransactions(ctx, txs)
		if err == nil {
			log.Debug(ctx, "successfully inserted batch", zap.Int("from", idx), zap.Int("to", idx+len(txs)))
		}
		errc <- err
	}

	for i := 0; i < len(batch); i += bf.BatchSize * bf.ParallelBatch {
		min, max := i, i+bf.BatchSize*bf.ParallelBatch
		if max > len(batch) {
			max = len(batch)
		}

		var n = 0
		for j := 0; j < bf.ParallelBatch; j++ {
			minBatch := min + bf.BatchSize*j
			maxBatch := min + bf.BatchSize*(j+1)
			if maxBatch > max {
				maxBatch = max
			}
			if minBatch > max {
				break
			}

			go f(batch[minBatch:maxBatch], minBatch)
			n++
		}

		var hasErr bool
		var aggrErr error
		for i := 0; i < n; i++ {
			var err = <-errc
			if err != nil {
				hasErr = true
				if aggrErr != nil {
					aggrErr = errors.Wrapf(aggrErr, err.Error())
				} else {
					aggrErr = err
				}
			}
		}

		if hasErr {
			return aggrErr
		}
	}

	return nil
}

func (bf *BlockFetcher) reorgs(ctx context.Context, processedBlock uint64, log *logger.ContextLogger) (bool, uint64) {
	blockNumber := processedBlock
	previousBlock := blockNumber - 1
	var blocksToDelete []uint64
	for {
		previousStoredBlock, err := bf.BlockStore.GetBlock(ctx, previousBlock)
		if err != nil {
			log.Error(ctx, "could not get block from store", zap.Error(err))
			return false, 0
		}
		log.Debug(ctx, "got block from blockstore", zap.Uint64("block_number", previousStoredBlock.Number), zap.Stringp("block_hash", previousStoredBlock.Hash))

		block, err := bf.Client.GetBlock(ctx, blockNumber)
		if err != nil {
			log.Error(ctx, "could not get block", zap.Error(err))
			return false, 0
		}
		log.Debug(ctx, "got block from blockchain", zap.Uint64("block_number", block.Number), zap.Stringp("block_hash", block.Hash), zap.Stringp("previous_hash", block.PreviousHash))

		if previousStoredBlock.Hash == nil {
			return false, 0
		}
		if block.PreviousHash == nil {
			return false, 0
		}

		if *previousStoredBlock.Hash == *block.PreviousHash {
			break
		}

		// Delete transactions
		log.Debug(ctx, "deleting block transactions", zap.Uint64("block_number", previousBlock))
		err = bf.TransactionStore.DeleteBlockTransactions(ctx, previousBlock)
		if err != nil {
			return false, 0
		}
		log.Debug(ctx, "successfully deleted block transactions", zap.Uint64("block_number", previousBlock))

		blocksToDelete = append(blocksToDelete, previousBlock)
		blockNumber--
		previousBlock--
	}

	// There was no reorgs.
	if blockNumber == processedBlock {
		return false, 0
	}

	// Delete blocks in blocktable
	log.Debug(ctx, "deleting blocks", zap.Uint64s("blocks", blocksToDelete))
	err := bf.BlockStore.DeleteBlocks(ctx, blocksToDelete)
	if err != nil {
		return false, 0
	}
	log.Debug(ctx, "successfully deleted blocks", zap.Uint64s("blocks", blocksToDelete))

	return true, previousBlock
}
