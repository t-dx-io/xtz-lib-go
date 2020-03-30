package cockroach

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/t-dx/tg-blocksd/internal/utils/database"
	pool "github.com/t-dx/tg-blocksd/internal/worker"
	common_model "github.com/t-dx/tg-blocksd/pkg/common/model"
	"github.com/t-dx/tg-blocksd/pkg/helper"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"

	"github.com/pkg/errors"
)

// TransactionStorage is the handler through which a CockroachDB backend can be queried.
type TransactionStorage struct {
	db database.DB
}

// NewTransactionStorage returns a fresh transaction service storage instance.
func NewTransactionStorage(db database.DB) *TransactionStorage {
	return &TransactionStorage{db: db}
}

// CreateTransactions saves the provided transactions objects in the database 'xtz_tx' table.
func (s *TransactionStorage) CreateTransactions(ctx context.Context, transactions []*model.Transaction) error {
	var begin = `INSERT INTO xtz_tx (hash, idx, block_number, addr_to, addr_from, amount, fee, counter, timestamp, pinned, broadcasted, status, created_at) VALUES `
	var conflict = `ON CONFLICT(hash, idx) DO UPDATE SET (block_number, addr_to, addr_from, amount, fee, counter, timestamp, status)=(excluded.block_number, excluded.addr_to, excluded.addr_from, excluded.amount, excluded.fee, excluded.counter, excluded.timestamp, excluded.status);`

	if len(transactions) == 0 {
		return nil
	}

	now := time.Now()

	var values = ""
	for _, tx := range transactions {
		switch {
		case tx == nil:
			return errors.New("transaction should not be nil")
		case tx.Amount == nil:
			return errors.New("transaction.Amount should not be nil")
		case !helper.IsBase64Alphabet(tx.Hash):
			return errors.New("Invalid character detected in transaction hash")
		}
		values = values + fmt.Sprintf(`('%s', %d, %s, %s, %s, %s, %s, %s, %s, %s, false, %d, %s),`,
			tx.Hash, tx.Index, database.Uint64OrNull(tx.BlockNumber), database.StringOrNull(tx.DestinationAddress), database.StringOrNull(tx.SourceAddress),
			database.BigIntOrNull(tx.Amount), database.BigIntOrNull(tx.Fee), database.BigIntOrNull(tx.Counter),
			database.FormattedTimestampOrNull(tx.Timestamp), database.FormattedBool(tx.Pinned), common_model.ToStatus(tx.Status), database.FormattedTimestampOrNull(&now))
	}
	values = values[:len(values)-1]

	var query = begin + values + conflict
	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return errors.Wrapf(err, "could not execute sql batch statement")
	}
	return nil
}

// GetTransactions queries stocked transactions for the given hashes.
func (s *TransactionStorage) GetTransactions(ctx context.Context, hashes []string) ([]*model.Transaction, error) {
	var query = `
SELECT id, hash, idx, block_number, addr_to, addr_from, amount, fee, counter, timestamp, pinned, broadcasted, rawtx, status, message, created_at, created_at_block, broadcasted_at_block
FROM xtz_tx
WHERE hash in (%[1]s);
`
	if len(hashes) == 0 {
		return []*model.Transaction{}, nil
	}

	var args string
	for _, hash := range hashes {
		if !helper.IsBase64Alphabet(hash) {
			return nil, errors.Errorf("invalid character detected in hash %q", hash)
		}

		args += fmt.Sprintf("'%s',", hash)
	}
	// Remove trailing comma.
	args = args[:len(args)-1]

	var storedTransactions []*transaction
	if err := s.db.Select(&storedTransactions, fmt.Sprintf(query, args)); err != nil {
		return nil, err
	}
	// Read repare. If the status is success, the error message should be nil.
	// If the transaction failed, then succeed, it is not the case.
	for _, storedTransaction := range storedTransactions {
		if storedTransaction.Status == common_model.SUCCESS && storedTransaction.Message != nil {
			s.eraseErrorMessage(ctx, storedTransaction.Hash)
			storedTransaction.Message = nil
		}
	}

	return toModelTransactions(storedTransactions), nil
}

func (s *TransactionStorage) eraseErrorMessage(ctx context.Context, hash string) {
	query := `
UPDATE xtz_tx
SET message = NULL
WHERE hash = $1
`
	_, _ = s.db.ExecContext(ctx, query, hash)
}

// GetTransactionsBetweenBlocks queries stocked transactions for a given address and block numbers.
func (s *TransactionStorage) GetTransactionsBetweenBlocks(ctx context.Context, addresses []string, fromBlock, toBlock uint64, limit, offset uint64) ([]*model.Transaction, uint64, error) {
	const query = `
SELECT * FROM (
  SELECT id, hash, idx, block_number, addr_to, addr_from, amount, fee, counter, timestamp, pinned, broadcasted, rawtx, status, message, created_at, created_at_block, broadcasted_at_block
  FROM xtz_tx
  WHERE addr_from in (%[1]s)
	AND block_number >= $1
	AND block_number <= $2
  UNION SELECT id, hash, idx, block_number, addr_to, addr_from, amount, fee, counter, timestamp, pinned, broadcasted, rawtx, status, message, created_at, created_at_block, broadcasted_at_block
  FROM xtz_tx
  WHERE addr_to in (%[1]s)
	AND block_number >= $1
	AND block_number <= $2
) LIMIT $3 OFFSET $4;
`
	const countQuery = `
SELECT count(*) FROM (
  SELECT hash
  FROM xtz_tx
  WHERE addr_from in (%[1]s)
	AND block_number >= $1
	AND block_number <= $2
  UNION SELECT hash
  FROM xtz_tx
  WHERE addr_to in (%[1]s)
	AND block_number >= $1
	AND block_number <= $2
);
`
	if len(addresses) == 0 {
		return []*model.Transaction{}, 0, nil
	}

	var args string
	for _, address := range addresses {
		if !helper.IsBase64Alphabet(address) {
			return nil, 0, errors.Errorf("invalid character detected in address %q", address)
		}

		args += fmt.Sprintf("'%s',", address)
	}
	// Remove trailing comma.
	args = args[:len(args)-1]

	var storedTransactions []*transaction
	if err := s.db.Select(&storedTransactions, fmt.Sprintf(query, args), fromBlock, toBlock, limit, offset); err != nil {
		return nil, 0, err
	}
	// Read repare. If the status is success, the error message should be nil.
	// If the transaction failed, then succeed, it is not the case.
	for _, storedTransaction := range storedTransactions {
		if storedTransaction.Status == common_model.SUCCESS && storedTransaction.Message != nil {
			s.eraseErrorMessage(ctx, storedTransaction.Hash)
			storedTransaction.Message = nil
		}
	}
	transactions := toModelTransactions(storedTransactions)

	var count uint64
	if err := database.QueryRowContext(ctx, s.db, fmt.Sprintf(countQuery, args), database.WithArgs(fromBlock, toBlock), database.WithDest(&count)); err != nil {
		return nil, 0, err
	}

	return transactions, count, nil
}

// GetTransactionsBetweenDates queries stocked transactions for a given address and dates.
func (s *TransactionStorage) GetTransactionsBetweenDates(ctx context.Context, addresses []string, fromDate, toDate time.Time, limit, offset uint64) ([]*model.Transaction, uint64, error) {
	const query = `
SELECT * FROM (
  SELECT id, hash, idx, block_number, addr_to, addr_from, amount, fee, counter, timestamp, pinned, broadcasted, rawtx, status, message, created_at, created_at_block, broadcasted_at_block
  FROM xtz_tx
  WHERE addr_from in (%[1]s)
	AND timestamp >= $1
	AND timestamp <= $2
  UNION SELECT id, hash, idx, block_number, addr_to, addr_from, amount, fee, counter, timestamp, pinned, broadcasted, rawtx, status, message, created_at, created_at_block, broadcasted_at_block
  FROM xtz_tx
  WHERE addr_to in (%[1]s)
	AND timestamp >= $1
	AND timestamp <= $2
) LIMIT $3 OFFSET $4;
`
	const countQuery = `
SELECT count(*) FROM (
  SELECT hash
  FROM xtz_tx
  WHERE addr_from in (%[1]s)
	AND timestamp >= $1
	AND timestamp <= $2
  UNION SELECT hash
  FROM xtz_tx
  WHERE addr_to in (%[1]s)
	AND timestamp >= $1
	AND timestamp <= $2
);
`
	if len(addresses) == 0 {
		return []*model.Transaction{}, 0, nil
	}

	var args string
	for _, address := range addresses {
		if !helper.IsBase64Alphabet(address) {
			return nil, 0, errors.Errorf("invalid character detected in address %q", address)
		}

		args += fmt.Sprintf("'%s',", address)
	}
	// Remove trailing comma.
	args = args[:len(args)-1]

	var storedTransactions []*transaction
	if err := s.db.Select(&storedTransactions, fmt.Sprintf(query, args), fromDate.UTC(), toDate.UTC(), limit, offset); err != nil {
		return nil, 0, err
	}
	// Read repare. If the status is success, the error message should be nil.
	// If the transaction failed, then succeed, it is not the case.
	for _, storedTransaction := range storedTransactions {
		if storedTransaction.Status == common_model.SUCCESS && storedTransaction.Message != nil {
			s.eraseErrorMessage(ctx, storedTransaction.Hash)
			storedTransaction.Message = nil
		}
	}
	transactions := toModelTransactions(storedTransactions)

	var count uint64
	if err := database.QueryRowContext(ctx, s.db, fmt.Sprintf(countQuery, args), database.WithArgs(fromDate.UTC(), toDate.UTC()), database.WithDest(&count)); err != nil {
		return nil, 0, err
	}

	return transactions, count, nil
}

//nolint:gosec
func (s *TransactionStorage) MarkPinned(ctx context.Context, addresses []string) error {
	batchSize := 1000

	for i := 0; i < len(addresses); i += batchSize {
		min, max := i, i+batchSize
		if max > len(addresses) {
			max = len(addresses)
		}

		var query string
		for _, address := range addresses[min:max] {
			query += fmt.Sprintf("UPDATE xtz_tx SET pinned = true WHERE addr_from = '%[1]s' AND pinned = false;\n"+
				"UPDATE xtz_tx SET pinned = true WHERE addr_to = '%[1]s' AND pinned = false;\n",
				address,
			)
		}

		_, err := s.db.ExecContext(ctx, query)
		if err != nil {
			return errors.Wrapf(err, "failed to batch mark pinned")
		}
	}

	return nil
}

func (s *TransactionStorage) Broadcast(ctx context.Context, transaction *model.Transaction) error {
	query := `
INSERT INTO xtz_tx (hash, idx, block_number, pinned, broadcasted, status, rawtx, timestamp, created_at, created_at_block, broadcasted_at_block)
VALUES(:hash, 0, -1, false, true, 0, :rawtx, :timestamp, NOW(), :created_at_block, 0)
ON CONFLICT (hash, idx) DO UPDATE SET (broadcasted, status, message, created_at_block, broadcasted_at_block) = (true, excluded.status, NULL, excluded.created_at_block, 0);
`
	if _, err := s.db.NamedExecContext(ctx, query, transaction); err != nil {
		return err
	}
	return nil
}

func (s *TransactionStorage) GetPendingBroadcasts(ctx context.Context, broadcastedBeforeBlock, limit uint64) ([]*model.Transaction, error) {
	query := fmt.Sprintf(`
SELECT hash, status, rawtx
FROM xtz_tx
WHERE broadcasted = true AND status IN (%d, %d, %d) AND block_number = -1 AND broadcasted_at_block <= $1 LIMIT $2;
`, common_model.NEW, common_model.PENDING, common_model.FAILURE)

	var storedTransactions []*transaction
	if err := s.db.Select(&storedTransactions, query, broadcastedBeforeBlock, limit); err != nil {
		return nil, err
	}

	return toModelTransactions(storedTransactions), nil
}

// UpdateBroadcast updates a broadcasted transaction with a new timestamp, status and error message.
func (s *TransactionStorage) UpdateBroadcast(ctx context.Context, hash, status, message string, broadcastedAtBlock uint64) error {
	const query = `
UPDATE xtz_tx SET (broadcasted_at_block, status, message) = ($2, $3, $4)
WHERE hash = $1
`
	st := common_model.ToStatus(status)
	if _, err := s.db.ExecContext(ctx, query, hash, broadcastedAtBlock, strconv.Itoa(int(st)), message); err != nil {
		return err
	}
	return nil
}

func (s *TransactionStorage) GetBroadcastsToGarbageCollect(ctx context.Context, beforeBlock uint64) ([]string, error) {
	query := fmt.Sprintf(`
SELECT hash
FROM xtz_tx@xtz_tx_broadcasted_status_block_number_broadcasted_at_block_idx
WHERE broadcasted = true AND status IN (%d, %d) AND block_number = -1 AND created_at_block <= $1;
`, common_model.PENDING, common_model.FAILURE)

	var hashes []string
	if err := s.db.Select(&hashes, query, beforeBlock); err != nil {
		return nil, err
	}

	return hashes, nil
}

func (s *TransactionStorage) GarbageCollectBroadcasts(ctx context.Context, broadcastHashes []string) error {
	query := `
UPDATE xtz_tx SET status = $1
WHERE hash in (%[1]s);
`
	batchSize := 100
	for {
		if len(broadcastHashes) == 0 {
			break
		}

		max := batchSize
		if max > len(broadcastHashes) {
			max = len(broadcastHashes)
		}

		var args string
		for _, broadcastHash := range broadcastHashes {
			args += fmt.Sprintf("'%s',", broadcastHash)
		}
		// Remove trailing comma.
		args = args[:len(args)-1]

		if _, err := s.db.ExecContext(ctx, fmt.Sprintf(query, args), common_model.TIMEOUT); err != nil {
			return err
		}

		broadcastHashes = broadcastHashes[max:]
	}
	return nil
}

func (s *TransactionStorage) GarbageCollectTransactions(ctx context.Context, beforeBlock uint64) error {
	for {
		// Get IDs
		ids, err := s.getTransactionIDs(ctx, 50000, beforeBlock)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}

		// Delete IDs
		type batch struct {
			ids []string
			idx int
		}

		var batcher = func(from, to int) interface{} {
			return &batch{ids: ids[from:to], idx: from}
		}

		var deleteBatchWorker = func(ctx context.Context, i interface{}) error {
			b, ok := i.(*batch)
			if !ok {
				return errors.Errorf("wrong type %T, should be *batch", i)
			}

			return s.deleteTransactionIDs(ctx, b.ids)
		}

		err = pool.Do(ctx, deleteBatchWorker, 100, batcher, len(ids), 100)
		if err != nil {
			return err
		}
	}
}

func (s *TransactionStorage) getTransactionIDs(ctx context.Context, limit int, beforeBlock uint64) ([]string, error) {
	const query = "SELECT id FROM xtz_tx WHERE pinned = false AND broadcasted = false AND block_number <= $1 LIMIT $2"
	rows, err := s.db.QueryContext(ctx, query, beforeBlock, limit)
	if err != nil {
		return nil, errors.Wrapf(err, "could not query id")
	}
	defer database.Close(ctx, rows)

	var ids []string
	for rows.Next() {
		var id string
		err := rows.Scan(&id)
		if err != nil {
			return nil, errors.Wrapf(err, "could not scan id")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

//nolint:gosec
func (s *TransactionStorage) deleteTransactionIDs(ctx context.Context, ids []string) error {
	for i, id := range ids {
		ids[i] = fmt.Sprintf("'%s'", id)
	}
	var query = fmt.Sprintf("DELETE FROM xtz_tx WHERE id IN (%s)", strings.Join(ids, ","))
	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return errors.Wrapf(err, "could not exec delete by ids")
	}
	return nil
}

//nolint:gosec
func (s *TransactionStorage) DumpPendingBroadcasts(ctx context.Context, limit, offset uint64, asOfSystemTime time.Time) ([]*model.Transaction, uint64, error) {
	query := fmt.Sprintf(`
SELECT hash, rawtx
FROM xtz_tx
AS OF SYSTEM TIME '%s'
WHERE broadcasted = true AND status IN (%d, %d, %d) LIMIT $1 OFFSET $2;
`, database.FormatSystemTime(asOfSystemTime), common_model.NEW, common_model.PENDING, common_model.FAILURE)

	countQuery := fmt.Sprintf(`
SELECT count(*)
FROM xtz_tx
AS OF SYSTEM TIME '%s'
WHERE broadcasted = true AND status IN (%d, %d, %d);
`, database.FormatSystemTime(asOfSystemTime), common_model.NEW, common_model.PENDING, common_model.FAILURE)

	var storedTransactions []*transaction
	if err := s.db.Select(&storedTransactions, query, limit, offset); err != nil {
		return nil, 0, err
	}

	var count uint64
	if err := database.QueryRowContext(ctx, s.db, countQuery, nil, database.WithDest(&count)); err != nil {
		return nil, 0, err
	}

	return toModelTransactions(storedTransactions), count, nil
}

//nolint:gosec
func (s *TransactionStorage) DumpPinnedTransactions(ctx context.Context, limit, offset uint64, asOfSystemTime time.Time) ([]*model.Transaction, uint64, error) {
	query := fmt.Sprintf(`
SELECT id, hash, idx, block_number, addr_to, addr_from, amount, fee, counter, timestamp, pinned, broadcasted, rawtx, status, message, created_at
FROM xtz_tx
AS OF SYSTEM TIME '%s'
WHERE pinned = true LIMIT $1 OFFSET $2;
`, database.FormatSystemTime(asOfSystemTime))

	countQuery := fmt.Sprintf(`
SELECT count(*)
FROM xtz_tx
AS OF SYSTEM TIME '%s'
WHERE pinned = true;
`, database.FormatSystemTime(asOfSystemTime))

	var storedTransactions []*transaction
	if err := s.db.Select(&storedTransactions, query, limit, offset); err != nil {
		return nil, 0, err
	}

	var count uint64
	if err := database.QueryRowContext(ctx, s.db, countQuery, nil, database.WithDest(&count)); err != nil {
		return nil, 0, err
	}

	return toModelTransactions(storedTransactions), count, nil
}

func (s *TransactionStorage) DeleteBlockTransactions(ctx context.Context, blockNumber uint64) error {
	for {
		// Get IDs
		ids, err := s.getTransactionIDsForBlock(ctx, 50000, blockNumber)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}

		// Delete IDs
		type batch struct {
			ids []string
		}

		var batcher = func(from, to int) interface{} {
			return &batch{ids: ids[from:to]}
		}

		var deleteBatchWorker = func(ctx context.Context, i interface{}) error {
			b, ok := i.(*batch)
			if !ok {
				return errors.Errorf("wrong type %T, should be *batch", i)
			}

			return s.deleteTransactionIDs(ctx, b.ids)
		}

		err = pool.Do(ctx, deleteBatchWorker, 100, batcher, len(ids), 100)
		if err != nil {
			return err
		}
	}
}

func (s *TransactionStorage) getTransactionIDsForBlock(ctx context.Context, limit int, blockNumber uint64) ([]string, error) {
	const query = "SELECT id FROM xtz_tx WHERE block_number = $1 LIMIT $2;"
	rows, err := s.db.QueryContext(ctx, query, blockNumber, limit)
	if err != nil {
		return nil, errors.Wrapf(err, "could not query id")
	}
	defer database.Close(ctx, rows)

	var ids []string
	for rows.Next() {
		var id string
		err := rows.Scan(&id)
		if err != nil {
			return nil, errors.Wrapf(err, "could not scan id")
		}
		ids = append(ids, id)
	}
	return ids, nil
}
