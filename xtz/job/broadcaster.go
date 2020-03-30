package job

import (
	"context"
	"fmt"
	"time"

	job "github.com/t-dx/go-jobs/v4"
	"github.com/t-dx/tg-blocksd/internal/logger"
	pool "github.com/t-dx/tg-blocksd/internal/worker"
	common_model "github.com/t-dx/tg-blocksd/pkg/common/model"
	common_service "github.com/t-dx/tg-blocksd/pkg/common/service"
	"github.com/t-dx/tg-blocksd/pkg/helper"
	"github.com/t-dx/tg-blocksd/pkg/xtz/client"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"
	xtz_service "github.com/t-dx/tg-blocksd/pkg/xtz/service"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const currency = "XTZ"

type Broadcaster struct {
	TransactionStore xtz_service.TransactionStore
	Client           xtz_service.Client

	BroadcastBlockInterval uint64
	BatchSize              uint64
	WorkersAmount          int

	BroadcastTrailsStore common_service.BroadcastTrailsStore

	MetricsTransactionsBroadcasted *prometheus.CounterVec
	MetricsJobDuration             *prometheus.SummaryVec
}

func (j *Broadcaster) Do(ctx context.Context, meta job.JobMeta, arg interface{}) (_ interface{}, _ map[string]string, err error) {
	log := logger.With(logger.TechLog, zap.String("job_name", meta.JobName), zap.String("job_id", meta.JobID))

	// Duration metrics
	defer func(begin time.Time) {
		status := "success"
		if err != nil {
			status = "failed"
		}
		j.MetricsJobDuration.With(helper.MakePrometheusLabels("name", meta.JobName, "status", status)).Observe(time.Since(begin).Seconds())
	}(time.Now())

	log.Info(ctx, "job started", zap.Time("now", time.Now().UTC()))

	height, err := j.Client.GetHeight(ctx)
	if err != nil {
		log.Error(ctx, "could not get last block", zap.Error(err))
		return nil, map[string]string{"msg": "could not get last block", "error": err.Error()}, err
	}
	blockNumber := height.Height

	if j.BroadcastBlockInterval > blockNumber {
		log.Error(ctx, "broadcast interval is greater than block number", zap.Uint64("block_number", blockNumber), zap.Uint64("broadcast_block_interval", j.BroadcastBlockInterval))
		return nil, map[string]string{"msg": "broadcast interval is greater than block number"}, err
	}
	broadcastedBeforeBlock := blockNumber - j.BroadcastBlockInterval

	// Get 100 top pending broadcasted tx before now - offset
	pendingTransactions, err := j.TransactionStore.GetPendingBroadcasts(ctx, broadcastedBeforeBlock, j.BatchSize)
	if err != nil {
		log.Error(ctx, "could not get pending broadcasts", zap.Error(err))
		return nil, map[string]string{"msg": "could not get pending broadcasts", "error": err.Error()}, err
	}

	// No work to do.
	if len(pendingTransactions) == 0 {
		log.Info(ctx, "no work to do")
		return nil, map[string]string{"msg": "no work to do"}, nil
	}

	log.Info(ctx, "successfully got broadcasts", zap.Int("broadcasts_amount", len(pendingTransactions)))

	// Process broadcasts with workers.
	workAmount := len(pendingTransactions)
	worker := func(ctx context.Context, i interface{}) error {
		transaction, ok := i.(*model.Transaction)
		if !ok {
			return errors.Errorf("wrong type %T, should be *model.Transaction", i)
		}

		status := common_model.PENDING
		message := ""
		if transaction.RawTransaction == nil {
			status = common_model.INVALID
			message = "raw transaction is nil"
		} else {
			log.Info(ctx, "broadcasting transaction", zap.String("hash", transaction.Hash), zap.String("raw_transaction", *transaction.RawTransaction))
			err := j.Client.BroadcastTransaction(ctx, *transaction.RawTransaction)
			if err != nil {
				log.Error(ctx, "broadcasting failed", zap.String("hash", transaction.Hash), zap.String("raw_transaction", *transaction.RawTransaction), zap.Error(err))
				status = common_model.FAILURE
				// If we get an error at first broadcast, it is considered a permanent error
				// and the status is put directly to invalid.
				if common_model.ToStatus(transaction.Status) == common_model.NEW {
					status = common_model.INVALID

					// If the error is temporary, set the status to Failure (i.e. it will be retried later).
					if _, ok := err.(*client.ErrBroadcastRetryable); ok {
						status = common_model.FAILURE
					}
				}
				message = fmt.Sprintf("could not send transaction %q: %v", transaction.Hash, err)
			} else {
				log.Info(ctx, "broadcasting success", zap.String("hash", transaction.Hash), zap.String("raw_transaction", *transaction.RawTransaction))
			}

			err = j.BroadcastTrailsStore.InsertBroadcastTrails(ctx, []*common_model.BroadcastTrail{{
				Currency:        currency,
				Action:          "broadcast",
				TransactionHash: transaction.Hash,
				BroadcastStatus: status.String(),
				Date:            time.Now().UTC(),
			}})
			if err != nil {
				log.Error(ctx, "unable to insert trail", zap.Error(err), zap.String("currency", currency), zap.String("action", "broadcast"), zap.String("transaction_hash", transaction.Hash), zap.String("status", status.String()))
			}
		}

		// Update broadcast in storage.
		return j.TransactionStore.UpdateBroadcast(ctx, transaction.Hash, common_model.FromStatus(status), message, blockNumber)
	}

	var workers []pool.Worker
	for i := 0; i < j.WorkersAmount; i++ {
		workers = append(workers, worker)
	}

	inputc, errc := pool.RegisterContext(ctx, workers, workAmount)
	for _, transaction := range pendingTransactions {
		inputc <- transaction
	}
	close(inputc)

	for err := range errc {
		if err != nil {
			log.Error(ctx, "could not broadcast transaction", zap.Error(err))
		}
	}

	// Metrics
	j.MetricsTransactionsBroadcasted.With(helper.MakePrometheusLabels("coin", currency)).Add(float64(workAmount))

	log.Info(ctx, "successfully finished")
	return nil, map[string]string{"msg": fmt.Sprintf("finished broadcasting %d transactions", len(pendingTransactions))}, nil
}
