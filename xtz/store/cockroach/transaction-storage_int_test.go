// +build integration

package cockroach_test

// Run the integration tests by including the "integration" tag:
// go test -v -tags integration -timeout 30s github.com/t-dx/tg-blocksd/pkg/xtz/store/cockroach

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"testing"
	"time"

	common_model "github.com/t-dx/tg-blocksd/pkg/common/model"
	helper "github.com/t-dx/tg-blocksd/pkg/helper_test"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"
	. "github.com/t-dx/tg-blocksd/pkg/xtz/store/cockroach"

	_ "github.com/lib/pq" // CockroachDB uses the Postgres SQL driver.

	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const currency = "xtz"

func TestCreateTransactions(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	ctx := context.Background()
	transactions := []*model.Transaction{
		{Hash: "op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW", BlockNumber: helper.FromUint64(560500), SourceAddress: helper.FromString("tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d"), DestinationAddress: helper.FromString("tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1"), Amount: big.NewInt(10), Timestamp: nowRounded(), CreatedAt: nowRounded()},
		{Hash: "ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ", BlockNumber: helper.FromUint64(560501), SourceAddress: helper.FromString("tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2"), DestinationAddress: helper.FromString("tz1Y6YwdGnrdTZ2NCiA6PDmRDk3BJUXXySrS"), Amount: big.NewInt(20), Timestamp: nowRounded(), CreatedAt: nowRounded()},
		{Hash: "op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc", BlockNumber: helper.FromUint64(560502), SourceAddress: helper.FromString("tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1"), DestinationAddress: helper.FromString("tz1TTFaFiW7xywionjQe9wxUrtk3cgHUDKix"), Amount: big.NewInt(30), Timestamp: nowRounded(), CreatedAt: nowRounded()},
		{Hash: "oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971", BlockNumber: helper.FromUint64(560503), SourceAddress: helper.FromString("tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR"), DestinationAddress: helper.FromString("tz1hkzS6pnfnHv9KzX1nbtqXVqUkzcem8FJs"), Amount: big.NewInt(40), Timestamp: nowRounded(), CreatedAt: nowRounded()},
		{Hash: "oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob", BlockNumber: helper.FromUint64(560504), SourceAddress: helper.FromString("tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe"), DestinationAddress: helper.FromString("tz1Pb2py4QrhS1u3KFqb6amZz87fCCcoLLFz"), Amount: big.NewInt(50), Timestamp: nowRounded(), CreatedAt: nowRounded()},
		{Hash: "onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ", BlockNumber: helper.FromUint64(560505), SourceAddress: helper.FromString("tz1SiPXX4MYGNJNDsRc7n8hkvUqFzg8xqF9m"), DestinationAddress: helper.FromString("tz1Ti7JwCpVHXcxwLTo4nBf3h6ivosNsD5uJ"), Amount: big.NewInt(60), Timestamp: nowRounded(), CreatedAt: nowRounded()},
		{Hash: "ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss", BlockNumber: helper.FromUint64(560506), SourceAddress: helper.FromString("tz1XFTtQKCUfZkE8nWpJEdFgy3PADSUio9fA"), DestinationAddress: helper.FromString("tz1ebZv2hxy1ABATkkgEajfgkjX9RuNJtTnY"), Amount: big.NewInt(70), Timestamp: nowRounded(), CreatedAt: nowRounded()},
		{Hash: "opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU", BlockNumber: helper.FromUint64(560507), SourceAddress: helper.FromString("tz1VwmmesDxud2BJEyDKUTV5T5VEP8tGBKGD"), DestinationAddress: helper.FromString("tz1gNjyzyT8L6WgNS4AdNMppsSFw76J4aDvT"), Amount: big.NewInt(80), Timestamp: nowRounded(), CreatedAt: nowRounded()},
		{Hash: "ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1", BlockNumber: helper.FromUint64(560508), SourceAddress: helper.FromString("tz1YCiftUM16FriwePPRx6V8A15ugAM5SXtr"), DestinationAddress: helper.FromString("tz1QF3YZLCVohjw2NLUEzKqswAh1TQt9MMhq"), Amount: big.NewInt(90), Timestamp: nowRounded(), CreatedAt: nowRounded()},
		{Hash: "ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2", BlockNumber: helper.FromUint64(560509), SourceAddress: helper.FromString("tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2"), DestinationAddress: helper.FromString("tz1bDhCGNZLQw1QXgf6MCzo6EtAVSGkqEB11"), Amount: big.NewInt(100), Timestamp: nowRounded(), CreatedAt: nowRounded()},
	}
	err := s.CreateTransactions(ctx, transactions)
	require.Nil(t, err)
}

func TestCreateTransactionsConflict(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	ctx := context.Background()
	q := `
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, pinned, timestamp, created_at) VALUES ('op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW', 0, -1, '10', true, 2, '0abcdef', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, pinned, timestamp, created_at) VALUES ('ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ', 0, -1, '20', true, 2, '1abcdef', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, pinned, timestamp, created_at) VALUES ('op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc', 0, -1, '30', true, 2, '2abcdef', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, pinned, timestamp, created_at) VALUES ('oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971', 0, -1, '40', true, 2, '3abcdef', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, pinned, timestamp, created_at) VALUES ('oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob', 0, -1, '50', true, 2, '4abcdef', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, pinned, timestamp, created_at) VALUES ('onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ', 0, -1, '60', true, 2, '5abcdef', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, pinned, timestamp, created_at) VALUES ('ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss', 0, -1, '70', true, 2, '6abcdef', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, pinned, timestamp, created_at) VALUES ('opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU', 0, -1, '80', true, 2, '7abcdef', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, pinned, timestamp, created_at) VALUES ('ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1', 0, -1, '90', true, 2, '8abcdef', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, pinned, timestamp, created_at) VALUES ('ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2', 0, -1, '100', true, 2, '9abcdef', false, NOW(), NOW());
`
	_, err := db.ExecContext(ctx, q)
	require.Nil(t, err)

	// Broadcasts are in the storage, now we insert transactions with same hashes.
	transactions := []*model.Transaction{
		{Hash: "op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW", BlockNumber: helper.FromUint64(560500), Amount: big.NewInt(10), Timestamp: nowRounded(), CreatedAt: nowRounded(), Status: common_model.SUCCESS.String()},
		{Hash: "ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ", BlockNumber: helper.FromUint64(560501), Amount: big.NewInt(20), Timestamp: nowRounded(), CreatedAt: nowRounded(), Status: common_model.SUCCESS.String()},
		{Hash: "op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc", BlockNumber: helper.FromUint64(560502), Amount: big.NewInt(30), Timestamp: nowRounded(), CreatedAt: nowRounded(), Status: common_model.SUCCESS.String()},
		{Hash: "oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971", BlockNumber: helper.FromUint64(560503), Amount: big.NewInt(40), Timestamp: nowRounded(), CreatedAt: nowRounded(), Status: common_model.SUCCESS.String()},
		{Hash: "oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob", BlockNumber: helper.FromUint64(560504), Amount: big.NewInt(50), Timestamp: nowRounded(), CreatedAt: nowRounded(), Status: common_model.SUCCESS.String()},
		{Hash: "onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ", BlockNumber: helper.FromUint64(560505), Amount: big.NewInt(60), Timestamp: nowRounded(), CreatedAt: nowRounded(), Status: common_model.SUCCESS.String()},
		{Hash: "ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss", BlockNumber: helper.FromUint64(560506), Amount: big.NewInt(70), Timestamp: nowRounded(), CreatedAt: nowRounded(), Status: common_model.SUCCESS.String()},
		{Hash: "opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU", BlockNumber: helper.FromUint64(560507), Amount: big.NewInt(80), Timestamp: nowRounded(), CreatedAt: nowRounded(), Status: common_model.SUCCESS.String()},
		{Hash: "ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1", BlockNumber: helper.FromUint64(560508), Amount: big.NewInt(90), Timestamp: nowRounded(), CreatedAt: nowRounded(), Status: common_model.SUCCESS.String()},
		{Hash: "ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2", BlockNumber: helper.FromUint64(560509), Amount: big.NewInt(100), Timestamp: nowRounded(), CreatedAt: nowRounded(), Status: common_model.SUCCESS.String()},
	}

	err = s.CreateTransactions(ctx, transactions)
	require.Nil(t, err)

	// Check that the transactions data has been updated.
	for _, transaction := range transactions {
		storedTransactions, err := s.GetTransactions(ctx, []string{transaction.Hash})
		require.Nil(t, err)
		require.Len(t, storedTransactions, 1)

		require.Equal(t, storedTransactions[0].BlockNumber, transaction.BlockNumber)
		require.Equal(t, storedTransactions[0].Status, "success")
		require.Equal(t, storedTransactions[0].Timestamp, transaction.Timestamp)
	}
}

func TestGetTransaction(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	ctx := context.Background()
	q := `
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW', 0, 500000, '10', true, 1, '0abcdef', 'message 0', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ', 0, 500001, '20', false, 1, '1abcdef', 'message 1', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc', 0, 500002, '30', true, 1, '2abcdef', 'message 2', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971', 0, 500003, '40', false, 1, '3abcdef', 'message 3', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob', 0, 500004, '50', true, 1, '4abcdef', 'message 4', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ', 0, 500005, '60', false, 0, '5abcdef', 'message 5', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss', 0, 500006, '70', true, 1, '6abcdef', 'message 6', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU', 0, 500007, '80', false, 2, '7abcdef', 'message 7', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1', 0, 500008, '90', true, 3, '8abcdef', 'message 8', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2', 0, -1, '100', false, 4, NULL, NULL, false, NOW(), NOW());
`
	_, err := db.ExecContext(ctx, q)
	require.Nil(t, err)

	tests := []struct {
		hash        string
		blockNumber *uint64
		broadcasted bool
		rawTx       *string
		status      string
		message     *string
		timestamp   *time.Time
		createdAt   *time.Time
	}{
		{hash: "op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW", blockNumber: helper.FromUint64(500000), broadcasted: true, rawTx: helper.FromString("0abcdef"), status: "success", message: helper.FromString("message 0"), timestamp: nowRounded(), createdAt: nowRounded()},
		{hash: "ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ", blockNumber: helper.FromUint64(500001), broadcasted: false, rawTx: helper.FromString("1abcdef"), status: "success", message: helper.FromString("message 1"), timestamp: nowRounded(), createdAt: nowRounded()},
		{hash: "op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc", blockNumber: helper.FromUint64(500002), broadcasted: true, rawTx: helper.FromString("2abcdef"), status: "success", message: helper.FromString("message 2"), timestamp: nowRounded(), createdAt: nowRounded()},
		{hash: "oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971", blockNumber: helper.FromUint64(500003), broadcasted: false, rawTx: helper.FromString("3abcdef"), status: "success", message: helper.FromString("message 3"), timestamp: nowRounded(), createdAt: nowRounded()},
		{hash: "oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob", blockNumber: helper.FromUint64(500004), broadcasted: true, rawTx: helper.FromString("4abcdef"), status: "success", message: helper.FromString("message 4"), timestamp: nowRounded(), createdAt: nowRounded()},
		{hash: "onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ", blockNumber: helper.FromUint64(500005), broadcasted: false, rawTx: helper.FromString("5abcdef"), status: "new", message: helper.FromString("message 5"), timestamp: nowRounded(), createdAt: nowRounded()},
		{hash: "ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss", blockNumber: helper.FromUint64(500006), broadcasted: true, rawTx: helper.FromString("6abcdef"), status: "success", message: helper.FromString("message 6"), timestamp: nowRounded(), createdAt: nowRounded()},
		{hash: "opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU", blockNumber: helper.FromUint64(500007), broadcasted: false, rawTx: helper.FromString("7abcdef"), status: "pending", message: helper.FromString("message 7"), timestamp: nowRounded(), createdAt: nowRounded()},
		{hash: "ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1", blockNumber: helper.FromUint64(500008), broadcasted: true, rawTx: helper.FromString("8abcdef"), status: "temporary_failure", message: helper.FromString("message 8"), timestamp: nowRounded(), createdAt: nowRounded()},
		{hash: "ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2", blockNumber: nil, broadcasted: false, rawTx: nil, status: "invalid", message: nil, timestamp: nowRounded(), createdAt: nowRounded()},
	}

	for _, test := range tests {
		transactions, err := s.GetTransactions(ctx, []string{test.hash})
		require.Nil(t, err)
		require.Len(t, transactions, 1)

		require.Equal(t, test.hash, transactions[0].Hash)
		require.Equal(t, test.blockNumber, transactions[0].BlockNumber)
		require.Equal(t, test.broadcasted, transactions[0].Broadcasted)
		require.Equal(t, test.rawTx, transactions[0].RawTransaction)
		require.Equal(t, test.status, transactions[0].Status)
		if transactions[0].Status == "success" {
			require.Nil(t, transactions[0].Message)
		} else {
			require.Equal(t, test.message, transactions[0].Message)
		}
	}

	// GetTransaction(h) when transaction with hash h is not in the database return an empty result.
	transactions, err := s.GetTransactions(ctx, []string{"012345679abcdef"})
	require.Nil(t, err)
	require.Equal(t, []*model.Transaction{}, transactions)
}

func TestGarbageCollectTransactions(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	ctx := context.Background()
	q := `
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW', 0, 500000, '10', false, 1, '0abcdef', 'message 0', false, TIMESTAMPTZ '2019-09-01 02:03:04', TIMESTAMPTZ '2019-10-01 02:03:04');
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ', 0, 500001, '20', false, 1, '1abcdef', 'message 1', false, TIMESTAMPTZ '2019-09-02 02:03:04', TIMESTAMPTZ '2019-11-02 02:03:04');
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc', 0, 500002, '30', false, 1, '2abcdef', 'message 2', false, TIMESTAMPTZ '2019-09-03 02:03:04', TIMESTAMPTZ '2019-10-03 02:03:04');
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971', 0, 500003, '40', false, 1, '3abcdef', 'message 3', false, TIMESTAMPTZ '2019-09-04 02:03:04', TIMESTAMPTZ '2019-10-04 02:03:04');
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob', 0, 500004, '50', true, 1, '4abcdef', 'message 4', false, TIMESTAMPTZ '2019-09-05 02:03:04', TIMESTAMPTZ '2019-10-05 02:03:04');
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ', 0, 500005, '60', false, 0, '5abcdef', 'message 5', false, TIMESTAMPTZ '2019-09-06 02:03:04', TIMESTAMPTZ '2019-10-06 02:03:04');
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss', 0, 500006, '70', false, 1, '6abcdef', 'message 6', false, TIMESTAMPTZ '2019-09-07 02:03:04', TIMESTAMPTZ '2019-10-07 02:03:04');
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU', 0, 500007, '80', false, 2, '7abcdef', 'message 7', false, TIMESTAMPTZ '2019-09-08 02:03:04', TIMESTAMPTZ '2019-11-08 02:03:04');
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1', 0, 500008, '90', false, 3, '8abcdef', 'message 8', false, TIMESTAMPTZ '2019-09-09 02:03:04', TIMESTAMPTZ '2019-10-09 02:03:04');
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2', 0, -1, '100', false, 4, NULL, NULL, false, TIMESTAMPTZ '2019-09-10 02:03:04', TIMESTAMPTZ '2019-10-10 02:03:04');
`
	_, err := db.ExecContext(ctx, q)
	require.Nil(t, err)

	beforeBlock := uint64(500006)
	err = s.GarbageCollectTransactions(ctx, beforeBlock)
	require.Nil(t, err)

	var tests = []struct {
		hash    string
		deleted bool
	}{
		{hash: "op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW", deleted: true},
		{hash: "ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ", deleted: true},
		{hash: "op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc", deleted: true},
		{hash: "oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971", deleted: true},
		{hash: "oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob", deleted: false}, // Broadcasted.
		{hash: "onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ", deleted: true},
		{hash: "ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss", deleted: true},
		{hash: "opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU", deleted: false}, // Block number.
		{hash: "ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1", deleted: false}, // Block number.
		{hash: "ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2", deleted: false}, // Block number.
	}

	for _, test := range tests {
		res, err := s.GetTransactions(ctx, []string{test.hash})
		require.Nil(t, err)
		if test.deleted {
			require.Equal(t, []*model.Transaction{}, res)
		}
	}
}

func TestDumpPendingBroadcasts(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	ctx := context.Background()

	q := `
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW', 0, 500000, '10', true, 0, '0abcdef', 'message 0', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ', 0, 500001, '20', true, 1, '1abcdef', 'message 1', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc', 0, 500002, '30', true, 2, '2abcdef', 'message 2', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971', 0, 500003, '40', true, 3, '3abcdef', 'message 3', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob', 0, 500004, '50', true, 4, '4abcdef', 'message 4', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ', 0, 500005, '60', true, 2, '5abcdef', 'message 5', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss', 0, 500006, '70', true, 2, '6abcdef', 'message 6', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU', 0, 500007, '80', true, 3, '7abcdef', 'message 7', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1', 0, 500008, '90', false, 3, '8abcdef', 'message 8', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2', 0, -1, '100', false, 4, NULL, NULL, false, NOW(), NOW());
`
	_, err := db.ExecContext(ctx, q)
	require.Nil(t, err)

	expectedBroadcasts := map[string]struct{}{
		"op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW": struct{}{},
		"op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc": struct{}{},
		"oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971": struct{}{},
		"onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ": struct{}{},
		"ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss": struct{}{},
		"opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU": struct{}{},
	}

	time.Sleep(time.Second) // Needed otherwise the query with as of system time return nothing.
	systemTime := time.Now()
	// Get first 3 broadcasts.
	limit, offset := uint64(3), uint64(0)
	broadcasts, count, err := s.DumpPendingBroadcasts(ctx, limit, offset, systemTime)
	require.Nil(t, err)
	require.Equal(t, uint64(6), count)
	for _, broadcast := range broadcasts {
		require.Contains(t, expectedBroadcasts, broadcast.Hash)
		delete(expectedBroadcasts, broadcast.Hash)
	}

	// Get next 2 broadcasts.
	limit, offset = 10, 3
	broadcasts, count, err = s.DumpPendingBroadcasts(ctx, limit, offset, systemTime)
	require.Nil(t, err)
	require.Equal(t, uint64(6), count)
	for _, broadcast := range broadcasts {
		require.Contains(t, expectedBroadcasts, broadcast.Hash)
		delete(expectedBroadcasts, broadcast.Hash)
	}

	// All broadcasts should be returned, and thus deleted from the map.
	require.Len(t, expectedBroadcasts, 0)
}

func TestDumpTransactions(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	ctx := context.Background()
	q := `
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW', 0, 500000, '10', true, 1, '0abcdef', 'message 0', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ', 0, 500001, '20', false, 1, '1abcdef', 'message 1', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc', 0, 500002, '30', true, 1, '2abcdef', 'message 2', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971', 0, 500003, '40', false, 1, '3abcdef', 'message 3', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob', 0, 500004, '50', true, 1, '4abcdef', 'message 4', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ', 0, 500005, '60', false, 0, '5abcdef', 'message 5', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss', 0, 500006, '70', true, 1, '6abcdef', 'message 6', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU', 0, 500007, '80', false, 2, '7abcdef', 'message 7', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1', 0, 500008, '90', true, 3, '8abcdef', 'message 8', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2', 0, -1, '100', false, 4, NULL, NULL, true, NOW(), NOW());
`
	_, err := db.ExecContext(ctx, q)
	require.Nil(t, err)

	expectedSubTransactions := map[string]struct{}{
		"op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW": struct{}{},
		"ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ": struct{}{},
		"op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc": struct{}{},
		"oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971": struct{}{},
		"onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ": struct{}{},
		"opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU": struct{}{},
		"ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1": struct{}{},
		"ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2": struct{}{},
	}

	time.Sleep(time.Second) // Needed otherwise the query with as of system time return nothing.
	systemTime := time.Now()
	// Get first 5 transactions.
	limit, offset := uint64(5), uint64(0)
	transactions, count, err := s.DumpPinnedTransactions(ctx, limit, offset, systemTime)
	require.Nil(t, err)
	require.Equal(t, uint64(8), count)
	for _, transaction := range transactions {
		require.Contains(t, expectedSubTransactions, transaction.Hash)
		delete(expectedSubTransactions, transaction.Hash)
	}

	// Get next 2 transactions.
	limit, offset = 2, 5
	transactions, count, err = s.DumpPinnedTransactions(ctx, limit, offset, systemTime)
	require.Nil(t, err)
	require.Equal(t, uint64(8), count)
	for _, transaction := range transactions {
		require.Contains(t, expectedSubTransactions, transaction.Hash)
		delete(expectedSubTransactions, transaction.Hash)
	}

	// Get next transaction.
	limit, offset = 10, 7
	transactions, count, err = s.DumpPinnedTransactions(ctx, limit, offset, systemTime)
	require.Nil(t, err)
	require.Equal(t, uint64(8), count)
	for _, transaction := range transactions {
		require.Contains(t, expectedSubTransactions, transaction.Hash)
		delete(expectedSubTransactions, transaction.Hash)
	}

	// All broadcasts should be returned, and thus deleted from the map.
	require.Len(t, expectedSubTransactions, 0)
}

func TestDeleteBlockTransactions(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	ctx := context.Background()
	q := `
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW', 0, 500000, '10', true, 1, '0abcdef', 'message 0', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ', 0, 500000, '20', false, 1, '1abcdef', 'message 1', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc', 0, 500000, '30', true, 1, '2abcdef', 'message 2', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971', 0, 500003, '40', false, 1, '3abcdef', 'message 3', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob', 0, 500000, '50', true, 1, '4abcdef', 'message 4', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ', 0, 500005, '60', false, 0, '5abcdef', 'message 5', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss', 0, 500006, '70', true, 1, '6abcdef', 'message 6', false, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU', 0, 500000, '80', false, 2, '7abcdef', 'message 7', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1', 0, 500008, '90', true, 3, '8abcdef', 'message 8', true, NOW(), NOW());
INSERT INTO xtz_tx (hash, idx, block_number, amount, broadcasted, status, rawtx, message, pinned, timestamp, created_at) VALUES ('ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2', 0, -1, '100', false, 4, NULL, NULL, true, NOW(), NOW());
`
	_, err := db.ExecContext(ctx, q)
	require.Nil(t, err)

	var count int
	err = db.Get(&count, "SELECT count(*) from xtz_tx")
	require.Nil(t, err)
	require.Equal(t, 10, count)

	err = s.DeleteBlockTransactions(ctx, 500000)
	require.Nil(t, err)

	err = db.Get(&count, "SELECT count(*) from xtz_tx")
	require.Nil(t, err)
	require.Equal(t, 5, count)
}

func nowRounded() *time.Time {
	t := time.Now().UTC().Round(time.Second)
	return &t
}

// ///////////////////////////
// OLDs interations tests
// ///////////////////////////

func TestIntLBPut(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var (
		ctx          = context.Background()
		transactions = randomEntries(100)
	)

	require.Nil(t, s.CreateTransactions(ctx, transactions))
	for _, transaction := range transactions {
		var rep, err = s.GetTransactions(ctx, []string{transaction.Hash})
		require.Nil(t, err)
		require.Len(t, rep, 1)

		require.Equal(t, transaction.Hash, rep[0].Hash)
		require.Equal(t, transaction.BlockNumber, rep[0].BlockNumber)
		require.Equal(t, transaction.DestinationAddress, rep[0].DestinationAddress)
		require.Equal(t, transaction.SourceAddress, rep[0].SourceAddress)
		require.Equal(t, transaction.Amount, rep[0].Amount)
		require.Equal(t, transaction.Fee, rep[0].Fee)
		require.False(t, rep[0].Pinned)
		require.False(t, rep[0].Broadcasted)
		require.Nil(t, rep[0].RawTransaction)
		require.Equal(t, "success", rep[0].Status)
		require.Nil(t, rep[0].Message)
		require.True(t, time.Now().After(*rep[0].CreatedAt))
	}
}

func TestIntLBGet(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var (
		ctx          = context.Background()
		transactions = randomEntries(10)
	)

	// Put transactions in store.
	require.Nil(t, s.CreateTransactions(ctx, transactions))

	// Get Tx
	{
		for _, transaction := range transactions {
			var rep, err = s.GetTransactions(ctx, []string{transaction.Hash})
			require.Nil(t, err)
			require.Len(t, rep, 1)

			require.Equal(t, transaction.Hash, rep[0].Hash)
			require.Equal(t, transaction.BlockNumber, rep[0].BlockNumber)
			require.Equal(t, transaction.DestinationAddress, rep[0].DestinationAddress)
			require.Equal(t, transaction.SourceAddress, rep[0].SourceAddress)
			require.Equal(t, transaction.Amount, rep[0].Amount)
			require.Equal(t, transaction.Fee, rep[0].Fee)
			require.Equal(t, transaction.Timestamp, rep[0].Timestamp)
			require.False(t, rep[0].Pinned)
			require.False(t, rep[0].Broadcasted)
			require.Nil(t, rep[0].RawTransaction)
			require.Equal(t, "success", rep[0].Status)
			require.Nil(t, rep[0].Message)
			require.True(t, time.Now().After(*rep[0].CreatedAt))
		}
	}
}

func TestIntLBGetBetweenBlocks(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var (
		ctx          = context.Background()
		address1     = helper.FromString(randomHexString(20))
		address2     = helper.FromString(randomHexString(20))
		address3     = helper.FromString(randomHexString(20))
		transactions = []*model.Transaction{
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(0), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(1), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.February, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(2), Fee: big.NewInt(0), DestinationAddress: address2, SourceAddress: address3, Timestamp: helper.FromTimestamp(time.Date(2018, time.March, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(3), Fee: big.NewInt(0), DestinationAddress: address3, SourceAddress: address3, Timestamp: helper.FromTimestamp(time.Date(2018, time.April, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(4), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.May, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(5), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.June, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(6), Fee: big.NewInt(0), DestinationAddress: address3, SourceAddress: address3, Timestamp: helper.FromTimestamp(time.Date(2018, time.July, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(7), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.August, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(8), Fee: big.NewInt(0), DestinationAddress: address2, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.September, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(9), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.October, 0, 0, 0, 0, 0, time.UTC))},
		}
	)

	// Put transactions in store.
	require.Nil(t, s.CreateTransactions(ctx, transactions))

	var tsts = []struct {
		address       *string
		start         uint64
		end           uint64
		expectedNbrTx int
	}{
		{address: address1, start: 0, end: 0, expectedNbrTx: 1},
		{address: address1, start: 0, end: 1, expectedNbrTx: 2},
		{address: address1, start: 0, end: 2, expectedNbrTx: 2},
		{address: address1, start: 0, end: 3, expectedNbrTx: 2},
		{address: address1, start: 0, end: 4, expectedNbrTx: 3},
		{address: address1, start: 0, end: 5, expectedNbrTx: 4},
		{address: address1, start: 0, end: 6, expectedNbrTx: 4},
		{address: address1, start: 0, end: 7, expectedNbrTx: 5},
		{address: address1, start: 0, end: 8, expectedNbrTx: 5},
		{address: address1, start: 0, end: 9, expectedNbrTx: 6},

		{address: address2, start: 0, end: 0, expectedNbrTx: 1},
		{address: address2, start: 0, end: 1, expectedNbrTx: 2},
		{address: address2, start: 0, end: 2, expectedNbrTx: 3},
		{address: address2, start: 0, end: 3, expectedNbrTx: 3},
		{address: address2, start: 0, end: 4, expectedNbrTx: 4},
		{address: address2, start: 0, end: 5, expectedNbrTx: 5},
		{address: address2, start: 0, end: 6, expectedNbrTx: 5},
		{address: address2, start: 0, end: 7, expectedNbrTx: 6},
		{address: address2, start: 0, end: 8, expectedNbrTx: 7},
		{address: address2, start: 0, end: 9, expectedNbrTx: 8},

		{address: address3, start: 0, end: 0, expectedNbrTx: 0},
		{address: address3, start: 0, end: 1, expectedNbrTx: 0},
		{address: address3, start: 0, end: 2, expectedNbrTx: 1},
		{address: address3, start: 0, end: 3, expectedNbrTx: 2},
		{address: address3, start: 0, end: 4, expectedNbrTx: 2},
		{address: address3, start: 0, end: 5, expectedNbrTx: 2},
		{address: address3, start: 0, end: 6, expectedNbrTx: 3},
		{address: address3, start: 0, end: 7, expectedNbrTx: 3},
		{address: address3, start: 0, end: 8, expectedNbrTx: 3},
		{address: address3, start: 0, end: 9, expectedNbrTx: 3},
	}

	for _, tst := range tsts {
		var rep, totalItems, err = s.GetTransactionsBetweenBlocks(ctx, []string{*tst.address}, tst.start, tst.end, 100, 0)
		require.Nil(t, err)
		require.Equal(t, uint64(tst.expectedNbrTx), totalItems)
		require.Equal(t, tst.expectedNbrTx, len(rep), fmt.Sprintf("GetBetween date %q and %q", tst.start, tst.end))
	}
}

func TestIntLBGetBetweenDates(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var (
		ctx          = context.Background()
		address1     = helper.FromString(randomHexString(20))
		address2     = helper.FromString(randomHexString(20))
		address3     = helper.FromString(randomHexString(20))
		transactions = []*model.Transaction{
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(0), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(0), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.February, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(0), Fee: big.NewInt(0), DestinationAddress: address2, SourceAddress: address3, Timestamp: helper.FromTimestamp(time.Date(2018, time.March, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(0), Fee: big.NewInt(0), DestinationAddress: address3, SourceAddress: address3, Timestamp: helper.FromTimestamp(time.Date(2018, time.April, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(2), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.May, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(4), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.June, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(0), Fee: big.NewInt(0), DestinationAddress: address3, SourceAddress: address3, Timestamp: helper.FromTimestamp(time.Date(2018, time.July, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(17), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.August, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(20), Fee: big.NewInt(0), DestinationAddress: address2, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.September, 0, 0, 0, 0, 0, time.UTC))},
			{Hash: randomHexString(32), Amount: big.NewInt(0), BlockNumber: helper.FromUint64(100), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Timestamp: helper.FromTimestamp(time.Date(2018, time.October, 0, 0, 0, 0, 0, time.UTC))},
		}
	)

	// Put transactions in store.
	require.Nil(t, s.CreateTransactions(ctx, transactions))

	var tsts = []struct {
		address       *string
		start         time.Time
		end           time.Time
		expectedNbrTx int
	}{
		{address: address1, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 1},
		{address: address1, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.February, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 2},
		{address: address1, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.March, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 2},
		{address: address1, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.April, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 2},
		{address: address1, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.May, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 3},
		{address: address1, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.June, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 4},
		{address: address1, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.July, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 4},
		{address: address1, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.August, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 5},
		{address: address1, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.September, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 5},
		{address: address1, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.October, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 6},

		{address: address2, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 1},
		{address: address2, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.February, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 2},
		{address: address2, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.March, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 3},
		{address: address2, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.April, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 3},
		{address: address2, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.May, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 4},
		{address: address2, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.June, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 5},
		{address: address2, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.July, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 5},
		{address: address2, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.August, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 6},
		{address: address2, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.September, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 7},
		{address: address2, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.October, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 8},

		{address: address3, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 0},
		{address: address3, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.February, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 0},
		{address: address3, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.March, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 1},
		{address: address3, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.April, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 2},
		{address: address3, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.May, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 2},
		{address: address3, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.June, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 2},
		{address: address3, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.July, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 3},
		{address: address3, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.August, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 3},
		{address: address3, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.September, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 3},
		{address: address3, start: time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC), end: time.Date(2018, time.October, 0, 0, 0, 0, 0, time.UTC), expectedNbrTx: 3},
	}

	for _, tst := range tsts {
		var rep, totalItems, err = s.GetTransactionsBetweenDates(ctx, []string{*tst.address}, tst.start, tst.end, 100, 0)
		require.Nil(t, err)
		require.Equal(t, uint64(tst.expectedNbrTx), totalItems)
		require.Equal(t, tst.expectedNbrTx, len(rep), fmt.Sprintf("GetBetween date %q and %q", tst.start, tst.end))
	}
}

func TestIntLBMarkPinned(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var (
		ctx          = context.Background()
		address1     = helper.FromString(randomHexString(20))
		address2     = helper.FromString(randomHexString(20))
		address3     = helper.FromString(randomHexString(20))
		address4     = helper.FromString(randomHexString(20))
		address5     = helper.FromString(randomHexString(20))
		transactions = []*model.Transaction{
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(0), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(0), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address1, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(0), Fee: big.NewInt(0), DestinationAddress: address2, SourceAddress: address3, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(0), Fee: big.NewInt(0), DestinationAddress: address3, SourceAddress: address3, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(2), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(4), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(10), Fee: big.NewInt(0), DestinationAddress: address3, SourceAddress: address3, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(17), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(20), Fee: big.NewInt(0), DestinationAddress: address2, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(100), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(4), Fee: big.NewInt(0), DestinationAddress: address4, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(10), Fee: big.NewInt(0), DestinationAddress: address4, SourceAddress: address3, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(17), Fee: big.NewInt(0), DestinationAddress: address5, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(20), Fee: big.NewInt(0), DestinationAddress: address5, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(100), Fee: big.NewInt(0), DestinationAddress: address5, SourceAddress: address2, Amount: big.NewInt(0)},
		}
		expected = []bool{
			true,
			true,
			false,
			false,
			true,
			true,
			false,
			true,
			false,
			true,
			true,
			true,
			true,
			true,
			true,
		}
	)

	// Put transactions in store.
	require.Nil(t, s.CreateTransactions(ctx, transactions))

	// MarkPinned.
	require.Nil(t, s.MarkPinned(ctx, []string{*address1, *address4, *address5}))

	// Check boolean was set in storage.
	for i, transaction := range transactions {
		var rep, err = s.GetTransactions(ctx, []string{transaction.Hash})
		require.Nil(t, err)
		require.Len(t, rep, 1)
		require.Equal(t, rep[0].Pinned, expected[i])
	}
}

func TestIntLBGarbageCollect(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var (
		ctx          = context.Background()
		address1     = helper.FromString(randomHexString(20))
		address2     = helper.FromString(randomHexString(20))
		address3     = helper.FromString(randomHexString(20))
		transactions = []*model.Transaction{
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(500000), Timestamp: helper.FromTimestamp(time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC)), CreatedAt: helper.FromTimestamp(time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC)), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(500000), Timestamp: helper.FromTimestamp(time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC)), CreatedAt: helper.FromTimestamp(time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC)), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(500010), Timestamp: helper.FromTimestamp(time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC)), CreatedAt: helper.FromTimestamp(time.Date(2018, time.January, 0, 0, 0, 0, 0, time.UTC)), Fee: big.NewInt(0), DestinationAddress: address2, SourceAddress: address3, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(500010), Timestamp: helper.FromTimestamp(time.Date(2018, time.February, 0, 0, 0, 0, 0, time.UTC)), CreatedAt: helper.FromTimestamp(time.Date(2018, time.February, 0, 0, 0, 0, 0, time.UTC)), Fee: big.NewInt(0), DestinationAddress: address3, SourceAddress: address3, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(500020), Timestamp: helper.FromTimestamp(time.Date(2018, time.March, 0, 0, 0, 0, 0, time.UTC)), CreatedAt: helper.FromTimestamp(time.Date(2018, time.March, 0, 0, 0, 0, 0, time.UTC)), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(500020), Timestamp: helper.FromTimestamp(time.Date(2018, time.April, 0, 0, 0, 0, 0, time.UTC)), CreatedAt: helper.FromTimestamp(time.Date(2018, time.April, 0, 0, 0, 0, 0, time.UTC)), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(500030), Timestamp: helper.FromTimestamp(time.Date(2018, time.May, 0, 0, 0, 0, 0, time.UTC)), CreatedAt: helper.FromTimestamp(time.Date(2018, time.May, 0, 0, 0, 0, 0, time.UTC)), Fee: big.NewInt(0), DestinationAddress: address3, SourceAddress: address3, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(500030), Timestamp: helper.FromTimestamp(time.Date(2018, time.June, 0, 0, 0, 0, 0, time.UTC)), CreatedAt: helper.FromTimestamp(time.Date(2018, time.June, 0, 0, 0, 0, 0, time.UTC)), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(500040), Timestamp: helper.FromTimestamp(time.Date(2018, time.July, 0, 0, 0, 0, 0, time.UTC)), CreatedAt: helper.FromTimestamp(time.Date(2018, time.July, 0, 0, 0, 0, 0, time.UTC)), Fee: big.NewInt(0), DestinationAddress: address2, SourceAddress: address2, Amount: big.NewInt(0)},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(500040), Timestamp: helper.FromTimestamp(time.Date(2018, time.August, 0, 0, 0, 0, 0, time.UTC)), CreatedAt: helper.FromTimestamp(time.Date(2018, time.August, 0, 0, 0, 0, 0, time.UTC)), Fee: big.NewInt(0), DestinationAddress: address1, SourceAddress: address2, Amount: big.NewInt(0)},
		}
	)

	// Put transactions in store.
	for _, transaction := range transactions {
		require.Nil(t, s.CreateTransactions(ctx, []*model.Transaction{transaction}))

		var createdAt = transaction.CreatedAt.UTC().Format("2006-01-02 15:04:05.0000")
		_, err := db.ExecContext(ctx, fmt.Sprintf("UPDATE xtz_tx SET created_at = '%s' WHERE hash = '%s'", createdAt, transaction.Hash))
		require.Nil(t, err)
	}

	// Set one tx to broadcasted, one to pinned.
	{
		_, err := db.ExecContext(context.Background(), fmt.Sprintf("UPDATE xtz_tx SET broadcasted = true WHERE hash = '%s'", transactions[2].Hash))
		require.Nil(t, err)
		_, err = db.ExecContext(context.Background(), fmt.Sprintf("UPDATE xtz_tx SET pinned = true WHERE hash = '%s'", transactions[3].Hash))
		require.Nil(t, err)
	}

	var tsts = []struct {
		beforeBlock   uint64
		expectedNbrTx int
	}{
		{beforeBlock: 400000, expectedNbrTx: 10},
		{beforeBlock: 500000, expectedNbrTx: 8},
		{beforeBlock: 500010, expectedNbrTx: 8},
		{beforeBlock: 500020, expectedNbrTx: 6},
		{beforeBlock: 500030, expectedNbrTx: 4},
		{beforeBlock: 500040, expectedNbrTx: 2},
		{beforeBlock: 500050, expectedNbrTx: 2},
	}

	for _, tst := range tsts {
		require.Nil(t, s.GarbageCollectTransactions(ctx, tst.beforeBlock))

		var count int
		var rows, err = db.QueryContext(context.Background(), "SELECT count(*) from xtz_tx")
		require.Nil(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		require.Nil(t, rows.Scan(&count))
		require.Equal(t, tst.expectedNbrTx, count)
	}
}

func TestIntLBGCBroadcasts(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var (
		ctx   = context.Background()
		tests = [10]struct {
			createdAtBlock uint64
			blockNumber    *int64
		}{
			{createdAtBlock: 500000, blockNumber: helper.FromInt64(-1)},
			{createdAtBlock: 500010, blockNumber: helper.FromInt64(-1)},
			{createdAtBlock: 500020, blockNumber: helper.FromInt64(-1)},
			{createdAtBlock: 500030, blockNumber: helper.FromInt64(-1)},
			{createdAtBlock: 500040, blockNumber: helper.FromInt64(-1)},
			{createdAtBlock: 500050, blockNumber: helper.FromInt64(1)},
			{createdAtBlock: 500060, blockNumber: helper.FromInt64(-1)},
			{createdAtBlock: 500070, blockNumber: helper.FromInt64(1)},
			{createdAtBlock: 500080, blockNumber: helper.FromInt64(-1)},
			{createdAtBlock: 500090, blockNumber: helper.FromInt64(-1)},
		}

		transactions = randomBroadcastedEntries(10)
	)

	for i, tx := range transactions {
		tx.BlockNumber = helper.Uint64(tests[i].blockNumber)
		if *tests[i].blockNumber > -1 {
			require.Nil(t, s.CreateTransactions(ctx, []*model.Transaction{tx}))
		}
		require.Nil(t, s.Broadcast(ctx, tx))
	}

	for i, tx := range transactions {
		_, err := db.ExecContext(context.Background(), fmt.Sprintf("UPDATE xtz_tx SET created_at_block = %d, broadcasted = true, status = %d WHERE hash = '%s'", tests[i].createdAtBlock, common_model.PENDING, tx.Hash))
		require.Nil(t, err)
	}

	for i := range transactions {
		hashes, err := s.GetBroadcastsToGarbageCollect(ctx, tests[i].createdAtBlock)
		require.Nil(t, err)
		require.Nil(t, s.GarbageCollectBroadcasts(ctx, hashes))
		for j, tx := range transactions {
			var rep, err = s.GetTransactions(ctx, []string{tx.Hash})
			require.Nil(t, err)
			require.Len(t, rep, 1)
			if tx.BlockNumber != nil {
				require.Equal(t, "pending", rep[0].Status)
			} else if j <= i && tx.BlockNumber == nil {
				require.Equal(t, "timeout", rep[0].Status)
			}
		}
	}
}

func TestIntLBPutConflict(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var broadcastedTransactions = randomBroadcastedEntries(100)
	var ctx = context.Background()
	// Put transactions in store.
	require.Nil(t, s.CreateTransactions(ctx, broadcastedTransactions))

	var randomTransactions = randomEntries(100)
	for i := range broadcastedTransactions {
		randomTransactions[i].Hash = broadcastedTransactions[i].Hash
		randomTransactions[i].Amount = broadcastedTransactions[i].Amount
	}
	require.Nil(t, s.CreateTransactions(ctx, randomTransactions))

	// Check boolean was set in storage.
	for i := range randomTransactions {
		var rep, err = s.GetTransactions(ctx, []string{randomTransactions[i].Hash})
		require.Nil(t, err)
		require.Len(t, rep, 1)

		require.Equal(t, rep[0].Hash, broadcastedTransactions[i].Hash)
		require.Equal(t, rep[0].BlockNumber, randomTransactions[i].BlockNumber)
		require.Equal(t, rep[0].DestinationAddress, randomTransactions[i].DestinationAddress)
		require.Equal(t, rep[0].SourceAddress, randomTransactions[i].SourceAddress)
		require.Equal(t, rep[0].Fee, randomTransactions[i].Fee)
		require.Nil(t, rep[0].RawTransaction)
		require.False(t, rep[0].Broadcasted)
		require.Equal(t, rep[0].Status, "success")
		require.Nil(t, rep[0].Message)
	}
}

func TestIntLBUpdateBroadcast(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var ctx = context.Background()

	var broadcastedTransaction = randomBroadcastedEntries(1)[0]
	require.Nil(t, s.CreateTransactions(ctx, []*model.Transaction{broadcastedTransaction}))
	var (
		hash               = broadcastedTransaction.Hash
		newStatus          = "temporary_failure"
		newMessage         = helper.FromString("Dummy message")
		broadcasterAtBlock = uint64(10000)
	)
	require.Nil(t, s.UpdateBroadcast(ctx, hash, newStatus, *newMessage, broadcasterAtBlock))
	var txs, err = s.GetTransactions(ctx, []string{hash})
	require.Nil(t, err)
	require.Len(t, txs, 1)

	require.Equal(t, txs[0].Hash, hash)
	require.Equal(t, txs[0].Status, newStatus)
	require.Equal(t, txs[0].Message, newMessage)
	require.NotNil(t, txs[0].BroadcastedAtBlock)
	require.Equal(t, *txs[0].BroadcastedAtBlock, broadcasterAtBlock)
}

func TestIntLBBroadcast(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var ctx = context.Background()
	now := time.Now()

	var broadcastedTransaction = randomBroadcastedEntries(1)[0]
	require.Nil(t, s.Broadcast(ctx, broadcastedTransaction))
	var (
		hash = broadcastedTransaction.Hash
	)
	var txs, err = s.GetTransactions(ctx, []string{hash})
	require.Nil(t, err)

	require.Equal(t, txs[0].Hash, hash)
	require.Equal(t, txs[0].Timestamp, broadcastedTransaction.Timestamp)
	require.Equal(t, txs[0].Status, broadcastedTransaction.Status)
	require.Equal(t, txs[0].Message, broadcastedTransaction.Message)
	require.True(t, txs[0].CreatedAt.After(now))
}

func TestIntLBBroadcastConflict(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var ctx = context.Background()

	var btx = randomEntries(1)[0]
	require.Nil(t, s.CreateTransactions(ctx, []*model.Transaction{btx}))
	var (
		hash  = btx.Hash
		rawTx = randomHexString(64)
	)
	var stx = &model.Transaction{
		Hash:           hash,
		RawTransaction: &rawTx,
		Amount:         big.NewInt(rand.Int63()),
		Fee:            big.NewInt(rand.Int63()),
		Broadcasted:    true,
		Status:         "pending",
		Message:        helper.FromString("Failure"),
	}
	require.Nil(t, s.Broadcast(ctx, stx))
	var rep, err = s.GetTransactions(ctx, []string{btx.Hash})
	require.Nil(t, err)
	require.Len(t, rep, 1)

	require.Equal(t, rep[0].Hash, btx.Hash)
	require.Equal(t, rep[0].BlockNumber, btx.BlockNumber)
	require.Equal(t, rep[0].DestinationAddress, btx.DestinationAddress)
	require.Equal(t, rep[0].SourceAddress, btx.SourceAddress)
	require.Equal(t, rep[0].Fee, btx.Fee)
	require.Nil(t, rep[0].RawTransaction)
	require.Equal(t, rep[0].Broadcasted, true)
	require.Equal(t, rep[0].Status, "new")
	require.Nil(t, rep[0].Message)
}

func TestIntLBGetPendingBroadcasts(t *testing.T) {
	var db = helper.Setup(currency)
	defer helper.Cleanup(currency, db)

	s := NewTransactionStorage(db)

	var (
		ctx          = context.Background()
		timeZero     = time.Time{}.Add(time.Second)
		transactions = []*model.Transaction{
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero), Broadcasted: true, Status: "temporary_failure"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(10 * time.Second)), Broadcasted: true, Status: "pending"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(20 * time.Second)), Broadcasted: true, Status: "temporary_failure"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(30 * time.Second)), Broadcasted: true, Status: "new"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero), Broadcasted: false, Status: "pending"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(10 * time.Second)), Broadcasted: false, Status: "temporary_failure"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(20 * time.Second)), Broadcasted: false, Status: "temporary_failure"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(30 * time.Second)), Broadcasted: false, Status: "pending"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero), Broadcasted: true, Status: "invalid"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(10 * time.Second)), Broadcasted: true, Status: "invalid"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(20 * time.Second)), Broadcasted: false, Status: "invalid"},
			{Hash: randomHexString(32), BlockNumber: nil, RawTransaction: helper.FromString(randomHexString(64)), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(30 * time.Second)), Broadcasted: true, Status: "invalid"},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(1), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero), Broadcasted: true, Status: "temporary_failure"},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(1), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(10 * time.Second)), Broadcasted: true, Status: "temporary_failure"},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(1), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(20 * time.Second)), Broadcasted: true, Status: "pending"},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(1), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(30 * time.Second)), Broadcasted: true, Status: "pending"},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(1), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero), Broadcasted: true, Status: "invalid"},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(1), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(10 * time.Second)), Broadcasted: true, Status: "invalid"},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(1), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(20 * time.Second)), Broadcasted: false, Status: "invalid"},
			{Hash: randomHexString(32), BlockNumber: helper.FromUint64(1), Fee: big.NewInt(0), Amount: big.NewInt(0), Timestamp: helper.FromTimestamp(timeZero.Add(30 * time.Second)), Broadcasted: true, Status: "invalid"},
		}
	)
	// Put transactions in store.
	for _, transaction := range transactions {
		if transaction.Broadcasted && transaction.BlockNumber == nil {
			require.Nil(t, s.Broadcast(ctx, transaction))
		} else {
			if transaction.BlockNumber == nil {
				transaction.BlockNumber = helper.FromUint64(0)
			}
			require.Nil(t, s.CreateTransactions(ctx, []*model.Transaction{transaction}))
		}
		_, err := db.ExecContext(context.Background(), fmt.Sprintf("UPDATE xtz_tx SET broadcasted = %t, status = %d WHERE hash = '%s'", transaction.Broadcasted, common_model.ToStatus(transaction.Status), transaction.Hash))
		require.Nil(t, err)
	}

	var tests = []struct {
		beforeBlock uint64
		limitBy     uint64
		expectedTxN int
	}{
		{beforeBlock: 500031, limitBy: 4, expectedTxN: 4},
		{beforeBlock: 500031, limitBy: 2, expectedTxN: 2},
		{beforeBlock: 500030, limitBy: 3, expectedTxN: 3},
		{beforeBlock: 500030, limitBy: 1, expectedTxN: 1},
		{beforeBlock: 500020, limitBy: 2, expectedTxN: 2},
		{beforeBlock: 500020, limitBy: 1, expectedTxN: 1},
		{beforeBlock: 500010, limitBy: 1, expectedTxN: 1},
		{beforeBlock: 500010, limitBy: 0, expectedTxN: 0},
	}
	for _, test := range tests {
		var txs, err = s.GetPendingBroadcasts(ctx, test.beforeBlock, test.limitBy)
		require.Nil(t, err)
		require.Len(t, txs, test.expectedTxN)
	}
}

// return n random chunk entries.
func randomEntries(n int) []*model.Transaction {
	var entries = make([]*model.Transaction, n)
	for i := 0; i < n; i++ {

		entries[i] = &model.Transaction{
			Hash:               randomHexString(32),
			BlockNumber:        helper.FromUint64(uint64(rand.Int63())),
			DestinationAddress: helper.FromString(randomHexString(20)),
			SourceAddress:      helper.FromString(randomHexString(20)),
			Amount:             big.NewInt(rand.Int63()),
			Fee:                big.NewInt(rand.Int63()),
			Status:             "success",
			CreatedAt:          nowRounded(),
		}
	}
	return entries
}

// return n random chunk entries.
func randomBroadcastedEntries(n int) []*model.Transaction {
	var entries = make([]*model.Transaction, n)
	for i := 0; i < n; i++ {

		entries[i] = &model.Transaction{
			Hash:           randomHexString(32),
			Amount:         big.NewInt(rand.Int63()),
			Broadcasted:    true,
			BlockNumber:    helper.FromUint64(uint64(rand.Int63())),
			RawTransaction: helper.FromString(randomHexString(64)),
			CreatedAt:      nowRounded(),
			Timestamp:      nowRounded(),
			Status:         "new",
		}
	}
	return entries
}

func randomHexString(nbrBytes int) string {
	var b = make([]byte, nbrBytes)
	rand.Read(b)
	return fmt.Sprintf("0x%x", b)
}
