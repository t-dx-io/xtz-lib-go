// +build client

package client

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/t-dx/tg-blocksd/internal/config"

	"github.com/stretchr/testify/require"
)

var cfg = config.NodeClient{
	URL:           "https://teznode.letzbake.com",
	WorkersAmount: 10,
}

func Test_BroadcastTransaction(t *testing.T) {
	client, err := NewClient(cfg)
	require.Nil(t, err)

	ctx := context.Background()

	err = client.BroadcastTransaction(ctx, "85a9ef47f6b1cc1432faaf87a242b08a42ea9e0c552b73ad6751efa5a75440376e00b1c4383a317576851a825b86aa59dc030e2ecb38dc0be0ab1ebc5000ff00a31e81ac3425310e3274a4698a793b2839dc0afa5f5d8672a4ee19cec93d8b7aa354a82dcaf534deeeb6345daa296eab5dba0520a334cebc8ed1b8c1a4d15de399dd0ad6494e3e17fff88b416131ade7d0d79e00")
	require.NotNil(t, err)
}

func Test_GetRawTransactionHash(t *testing.T) {
	c, err := NewClient(cfg)
	require.Nil(t, err)

	var tests = []struct {
		rawTransaction string
		expectedHash   string
	}{
		{expectedHash: "ooV9NJ8uToUpaPV3ybvbF49gH8kFQ5E69XehwoMAPzeRVWmauba", rawTransaction: "85a9ef47f6b1cc1432faaf87a242b08a42ea9e0c552b73ad6751efa5a75440376e00b1c4383a317576851a825b86aa59dc030e2ecb38dc0be0ab1ebc5000ff00a31e81ac3425310e3274a4698a793b2839dc0afa5f5d8672a4ee19cec93d8b7aa354a82dcaf534deeeb6345daa296eab5dba0520a334cebc8ed1b8c1a4d15de399dd0ad6494e3e17fff88b416131ade7d0d79e00"},
	}

	for _, tst := range tests {
		hash, err := c.GetRawTransactionHash(context.Background(), tst.rawTransaction)
		require.Nil(t, err)
		require.Equal(t, tst.expectedHash, hash)
	}
}

func Test_GetEstimatedFee(t *testing.T) {
	client, err := NewClient(cfg)
	require.Nil(t, err)

	ctx := context.Background()

	fees, err := client.GetEstimatedFee(ctx)
	require.Nil(t, err)
	require.Equal(t, 0, big.NewInt(100000).Cmp(fees.MinimalFees))
	require.Equal(t, 0, big.NewInt(100).Cmp(fees.MinimalNanotezPerGasUnit))
	require.Equal(t, 0, big.NewInt(1000).Cmp(fees.MinimalNanotezPerByte))
}

func Test_GetBalance(t *testing.T) {
	client, err := NewClient(cfg)
	require.Nil(t, err)

	ctx := context.Background()
	addresses := []string{
		"tz3VEZ4k6a4Wx42iyev6i2aVAptTRLEAivNN",
		"tz3RB4aoyjov4KEVRbuhvQ1CKJgBJMWhaeB8",
		"tz3NExpXn9aPNZPorRE4SdjJ2RGrfbJgMAaV",
		"tz3bTdwZinP8U1JmSweNzVKhmwafqWmFWRfk",
	}
	blockNumber := uint64(868966)

	balances, err := client.GetBalances(ctx, addresses, blockNumber)
	require.Nil(t, err)
	require.Len(t, balances, len(addresses))
	for _, balance := range balances {
		require.Nil(t, balance.Error)
		require.Contains(t, addresses, balance.Address)
		require.NotNil(t, balance.BalanceAtBlock)
		require.NotNil(t, balance.BalanceAtTip)
	}
}

func Test_GetBlock(t *testing.T) {
	c, err := NewClient(cfg)
	require.Nil(t, err)

	var blockNumber uint64 = 868970

	block, err := c.GetBlock(context.Background(), blockNumber)
	require.Nil(t, err)
	require.Equal(t, blockNumber, block.Number)
	require.Equal(t, "BMG3U3bvvfCd91H6S5dWZPmReAjNGQPipJVqCyTpzCWYbPTNA8D", *block.Hash)
	require.Equal(t, "BMf1roX8PzzQ8T1FVR3je1V8DcnxYsqEqYnWjW9PcRjYJP3ARSu", *block.PreviousHash)
	require.Equal(t, time.Date(2020, time.March, 17, 9, 18, 0, 0, time.UTC), *block.Timestamp)
	require.Nil(t, block.CreatedAt)
}

func Test_GetHeight(t *testing.T) {
	c, err := NewClient(cfg)
	require.Nil(t, err)

	height, err := c.GetHeight(context.Background())
	require.Nil(t, err)
	require.Greater(t, height.Height, uint64(868970))
}

func Test_GetCounters(t *testing.T) {
	c, err := NewClient(cfg)
	require.Nil(t, err)

	addresses := []string{
		"tz3VEZ4k6a4Wx42iyev6i2aVAptTRLEAivNN",
		"tz3RB4aoyjov4KEVRbuhvQ1CKJgBJMWhaeB8",
		"tz3NExpXn9aPNZPorRE4SdjJ2RGrfbJgMAaV",
		"tz3bTdwZinP8U1JmSweNzVKhmwafqWmFWRfk",
	}

	counters, err := c.GetCounters(context.Background(), addresses)
	require.Nil(t, err)
	for _, balance := range counters {
		require.Nil(t, balance.Error)
		require.Contains(t, addresses, balance.Address)
		require.NotNil(t, balance.Counter)
	}
}

func Test_GetTransactions(t *testing.T) {
	c, err := NewClient(cfg)
	require.Nil(t, err)

	var blockNumber uint64 = 868984

	transactions, err := c.GetTransactions(context.Background(), blockNumber)

	for _, transaction := range transactions {
		fmt.Printf("Hash: %s, From: %s, To: %s\n", transaction.Hash, *transaction.SourceAddress, *transaction.DestinationAddress)
	}

	require.Nil(t, err)
	require.Len(t, transactions, 2)
}
