package client

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/t-dx/tg-blocksd/internal/config"
	pool "github.com/t-dx/tg-blocksd/internal/worker"
	common_model "github.com/t-dx/tg-blocksd/pkg/common/model"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"

	"github.com/btcsuite/btcutil/base58"
	gotezos "github.com/goat-systems/go-tezos/v2"
	"github.com/pkg/errors"
	"golang.org/x/crypto/blake2b"
)

// ErrBroadcastRetryable is a tempory error returned during transaction broadcast, e.g. mempool full.
type ErrBroadcastRetryable struct {
	msg string
}

func (e *ErrBroadcastRetryable) Error() string {
	return e.msg
}

// NewClient returns a new tezos client.
func NewClient(cfg config.NodeClient) (*Client, error) {
	client, err := gotezos.New(cfg.URL)
	if err != nil {
		return nil, err
	}

	workersAmount := cfg.WorkersAmount
	if workersAmount < 1 {
		workersAmount = 1
	}

	return &Client{client: client, workersAmount: workersAmount}, nil
}

// Client is tezos client.
type Client struct {
	client        *gotezos.GoTezos
	workersAmount int
}

var retryableErrorsMessages = []string{
	"Client.Timeout exceeded while awaiting headers",
}

var mainChainID = "main"

func (c *Client) BroadcastTransaction(ctx context.Context, rawTransaction string) error {
	_, err := c.client.InjectionOperation(&gotezos.InjectionOperationInput{
		ChainID:   &mainChainID,
		Operation: &rawTransaction,
	})
	if err != nil {
		for _, msg := range retryableErrorsMessages {
			if strings.Contains(err.Error(), msg) {
				return &ErrBroadcastRetryable{msg: fmt.Sprintf("temporary failure: %s", err.Error())}
			}
		}
		return err
	}

	return nil
}

var prefix = []byte{0x05, 0x74}

func (c *Client) GetRawTransactionHash(ctx context.Context, rawTransaction string) (string, error) {
	data, err := hex.DecodeString(rawTransaction)
	if err != nil {
		return "", err
	}

	// The hash is the base58check encoding of the prefix 0x0574 + blake2(raw transaction)
	sum := blake2b.Sum256(data)

	hash := []byte{}
	// Base58check function only allow 1 byte prefix, so we append prefix[1] here, and prefix[0] during the call to CheckEncode.
	hash = append(hash, prefix[1])
	hash = append(hash, sum[:]...)

	b58Hash := base58.CheckEncode(hash, prefix[0])

	return b58Hash, nil
}

var defaultMinimalFees = big.NewInt(100000)
var defaultMinimalNanotezPerGasUnit = big.NewInt(100)

func (c *Client) GetEstimatedFee(ctx context.Context) (*model.Fees, error) {
	res, err := c.client.Constants("head")
	if err != nil {
		return nil, err
	}

	return &model.Fees{
		MinimalFees:              defaultMinimalFees,
		MinimalNanotezPerGasUnit: defaultMinimalNanotezPerGasUnit,
		MinimalNanotezPerByte:    res.CostPerByte.Big,
	}, nil
}

func (c *Client) GetBalances(ctx context.Context, addresses []string, blockNumber uint64) ([]*model.Balance, error) {
	if len(addresses) == 0 {
		return []*model.Balance{}, nil
	}

	nWork := len(addresses)
	res := make(chan *model.Balance, nWork)
	var wg sync.WaitGroup
	wg.Add(nWork)

	go func() {
		wg.Wait()
		close(res)
	}()

	worker := func(ctx context.Context, i interface{}) error {
		address, ok := i.(string)
		if !ok {
			return errors.Errorf("wrong type %T, should be string", i)
		}

		balanceAtBlock, balanceAtTip, err := c.getBalances(ctx, address, blockNumber)
		if err != nil {
			res <- &model.Balance{
				Address: address,
				Error:   err,
			}
		} else {
			res <- &model.Balance{
				Address:        address,
				BalanceAtBlock: balanceAtBlock,
				BalanceAtTip:   balanceAtTip,
			}
		}

		wg.Done()
		return nil
	}

	var workers []pool.Worker
	for i := 0; i < c.workersAmount; i++ {
		workers = append(workers, worker)
	}

	var inputc, errc = pool.RegisterContext(ctx, workers, nWork)
	for _, address := range addresses {
		inputc <- address
	}
	close(inputc)

	var aggrErr error
	for err := range errc {
		if err != nil {
			if aggrErr != nil {
				aggrErr = errors.Wrap(aggrErr, err.Error())
			} else {
				aggrErr = err
			}
		}
	}

	if aggrErr != nil {
		return nil, aggrErr
	}

	balances := []*model.Balance{}
	for balance := range res {
		balances = append(balances, balance)
	}

	return balances, nil
}

func (c *Client) getBalances(_ context.Context, account string, blockNumber uint64) (*big.Int, *big.Int, error) {
	head, err := c.client.Head()
	if err != nil {
		return nil, nil, err
	}

	balanceAtTip, err := c.client.Balance(head.Hash, account)
	if err != nil {
		return nil, nil, err
	}

	// If blockNumber is not provided, the balance at tip is returned.
	if blockNumber == 0 {
		return balanceAtTip, balanceAtTip, nil
	}

	block, err := c.client.Block(int(blockNumber))
	if err != nil {
		return nil, nil, err
	}

	balanceAtBlock, err := c.client.Balance(block.Hash, account)
	if err != nil {
		return nil, nil, err
	}

	return balanceAtBlock, balanceAtTip, nil
}

func (c *Client) GetBlock(ctx context.Context, blockNumber uint64) (*common_model.Block, error) {
	block, err := c.client.Block(int(blockNumber))
	if err != nil {
		return nil, err
	}

	timestamp := block.Header.Timestamp.UTC()
	return &common_model.Block{
		Number:       blockNumber,
		Hash:         &block.Hash,
		PreviousHash: &block.Header.Predecessor,
		Timestamp:    &timestamp,
	}, nil
}

func (c *Client) GetHeight(ctx context.Context) (*model.Height, error) {
	head, err := c.client.Head()
	if err != nil {
		return nil, err
	}

	return &model.Height{
		Height: uint64(head.Header.Level),
		Hash:   head.Hash,
	}, nil
}

func (c *Client) GetCounters(ctx context.Context, addresses []string) ([]*model.Counter, error) {
	if len(addresses) == 0 {
		return []*model.Counter{}, nil
	}

	head, err := c.client.Head()
	if err != nil {
		return nil, err
	}

	nWork := len(addresses)
	res := make(chan *model.Counter, nWork)
	var wg sync.WaitGroup
	wg.Add(nWork)

	go func() {
		wg.Wait()
		close(res)
	}()

	worker := func(ctx context.Context, i interface{}) error {
		address, ok := i.(string)
		if !ok {
			return errors.Errorf("wrong type %T, should be string", i)
		}

		c, err := c.client.Counter(head.Hash, address)
		var counter uint64
		if c != nil {
			counter = uint64(*c)
		}

		if err != nil {
			res <- &model.Counter{
				Address: address,
				Error:   err,
			}
		} else {
			res <- &model.Counter{
				Address: address,
				Counter: counter,
			}
		}

		wg.Done()
		return nil
	}

	var workers []pool.Worker
	for i := 0; i < c.workersAmount; i++ {
		workers = append(workers, worker)
	}

	var inputc, errc = pool.RegisterContext(ctx, workers, nWork)
	for _, address := range addresses {
		inputc <- address
	}
	close(inputc)

	var aggrErr error
	for err := range errc {
		if err != nil {
			if aggrErr != nil {
				aggrErr = errors.Wrap(aggrErr, err.Error())
			} else {
				aggrErr = err
			}
		}
	}

	if aggrErr != nil {
		return nil, aggrErr
	}

	counters := []*model.Counter{}
	for counter := range res {
		counters = append(counters, counter)
	}

	return counters, nil
}

func (c *Client) GetTransactions(ctx context.Context, blockNumber uint64) ([]*model.Transaction, error) {
	block, err := c.client.Block(int(blockNumber))
	if err != nil {
		return nil, err
	}

	transactionIndex := map[string]uint64{}
	transactions := []*model.Transaction{}
	for _, operations := range block.Operations {
		for _, operation := range operations {
			for _, content := range operation.Contents {
				switch content.Kind {
				case "transaction":
					ts := block.Header.Timestamp.UTC()
					transactions = append(transactions, &model.Transaction{
						Hash:               operation.Hash,
						Index:              transactionIndex[operation.Hash],
						BlockNumber:        &blockNumber,
						SourceAddress:      &content.Source,
						DestinationAddress: &content.Destination,
						Amount:             content.Amount.Big,
						Counter:            content.Counter.Big,
						Fee:                content.Fee.Big,
						Timestamp:          &ts,
						Status:             common_model.SUCCESS.String(),
					})
					transactionIndex[operation.Hash]++
				}
			}
		}
	}

	return transactions, nil
}
