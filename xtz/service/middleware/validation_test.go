package middleware

import (
	"context"
	"testing"
	"time"

	val "github.com/t-dx/tg-blocksd/internal/utils/validation"
	"github.com/t-dx/tg-blocksd/pkg/xtz/model"
	"github.com/t-dx/tg-blocksd/pkg/xtz/service"

	"github.com/stretchr/testify/require"
)

func Test_XTZValidationAddAddresses(t *testing.T) {
	svc := Validation(val.NewValidator())(&mockXTZService{})

	ctx := context.Background()
	tests := []struct {
		req   *service.AddAddressesReq
		valid bool
	}{
		{
			req: &service.AddAddressesReq{
				Network: "mainnet",
				Addresses: []string{
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe"},
			},
			valid: true,
		},
		{
			req: &service.AddAddressesReq{
				Network: "mainnet",
				Addresses: []string{
					"tz1SiPXX4MYGNJNDsRc7n8hkvUqFzg8xqF9m",
					"tz1XFTtQKCUfZkE8nWpJEdFgy3PADSUio9fA",
					"tz1VwmmesDxud2BJEyDKUTV5T5VEP8tGBKGD",
					"tz1YCiftUM16FriwePPRx6V8A15ugAM5SXtr",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2"},
			},
			valid: true,
		},
		{
			req: &service.AddAddressesReq{
				Network: "wrongnetwork",
				Addresses: []string{
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe"},
			},
			valid: false,
		},
		{req: nil, valid: false},
		{req: &service.AddAddressesReq{}, valid: false},
		{req: &service.AddAddressesReq{Network: "mainnet"}, valid: false},
		{
			req: &service.AddAddressesReq{
				Network: "mainnet",
				Addresses: []string{
					"0xdac17f958d2ee523a2206206994597c13d831ec7", // wrong format
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe"},
			},
			valid: false,
		},
		{
			req: &service.AddAddressesReq{
				Network:   "mainnet",
				Addresses: []string{""},
			},
			valid: false,
		},
	}

	for i, test := range tests {
		err := svc.AddAddresses(ctx, test.req)
		if test.valid {
			require.Nil(t, err, i)
		} else {
			require.NotNil(t, err, i)
		}
	}
}

func Test_XTZValidationBroadcast(t *testing.T) {
	svc := Validation(val.NewValidator())(&mockXTZService{})

	ctx := context.Background()
	tests := []struct {
		req   *service.BroadcastByCustomerReq
		valid bool
	}{
		{
			req: &service.BroadcastByCustomerReq{
				Network:        "mainnet",
				CustomerID:     "daae03ef-fa60-4b22-9c10-552de333711a",
				RawTransaction: "85a9ef47f6b1cc1432faaf87a242b08a42ea9e0c552b73ad6751efa5a75440376e00b1c4383a317576851a825b86aa59dc030e2ecb38dc0be0ab1ebc5000ff00a31e81ac3425310e3274a4698a793b2839dc0afa5f5d8672a4ee19cec93d8b7aa354a82dcaf534deeeb6345daa296eab5dba0520a334cebc8ed1b8c1a4d15de399dd0ad6494e3e17fff88b416131ade7d0d79e00",
			},
			valid: true,
		},
		{req: &service.BroadcastByCustomerReq{
			Network:        "mainnet",
			CustomerID:     "", // missing customer ID
			RawTransaction: "85a9ef47f6b1cc1432faaf87a242b08a42ea9e0c552b73ad6751efa5a75440376e00b1c4383a317576851a825b86aa59dc030e2ecb38dc0be0ab1ebc5000ff00a31e81ac3425310e3274a4698a793b2839dc0afa5f5d8672a4ee19cec93d8b7aa354a82dcaf534deeeb6345daa296eab5dba0520a334cebc8ed1b8c1a4d15de399dd0ad6494e3e17fff88b416131ade7d0d79e00",
		},
			valid: false,
		},
		{req: nil, valid: false},
		{
			req: &service.BroadcastByCustomerReq{
				Network:        "mainnet",
				CustomerID:     "daae03ef-fa60-4b22-9c10-552de333711a",
				RawTransaction: "",
			},
			valid: false,
		},
	}

	for _, test := range tests {
		_, err := svc.Broadcast(ctx, test.req)
		if test.valid {
			require.Nil(t, err)
		} else {
			require.NotNil(t, err)
		}
	}
}

func Test_XTZValidationGetEstimatedFee(t *testing.T) {
	svc := Validation(val.NewValidator())(&mockXTZService{})

	ctx := context.Background()
	tests := []struct {
		req   *service.GetEstimatedFeeReq
		valid bool
	}{

		{req: nil, valid: false},
		{req: &service.GetEstimatedFeeReq{}, valid: false},
		{req: &service.GetEstimatedFeeReq{Network: "mainnet"}, valid: true},
	}

	for _, test := range tests {
		_, err := svc.GetEstimatedFee(ctx, test.req)
		if test.valid {
			require.Nil(t, err)
		} else {
			require.NotNil(t, err)
		}
	}
}

func Test_XTZValidationGetBalances(t *testing.T) {
	svc := Validation(val.NewValidator())(&mockXTZService{})

	ctx := context.Background()
	tests := []struct {
		req   *service.GetBalancesReq
		valid bool
	}{
		{
			req: &service.GetBalancesReq{
				Network: "mainnet",
				Addresses: []string{
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe"},
			},
			valid: true,
		},
		{
			req: &service.GetBalancesReq{
				Network: "wrongnetwork",
				Addresses: []string{
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe"},
			},
			valid: false,
		},
		{req: nil, valid: false},
		{req: &service.GetBalancesReq{}, valid: false},
		{req: &service.GetBalancesReq{Network: "mainnet"}, valid: false},
		{
			req: &service.GetBalancesReq{
				Network: "mainnet",
				Addresses: []string{
					"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756ccg", // invalid address
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe"},
			},
			valid: false,
		},
	}

	for _, test := range tests {
		_, err := svc.GetBalances(ctx, test.req)
		if test.valid {
			require.Nil(t, err)
		} else {
			require.NotNil(t, err)
		}
	}
}

func Test_XTZValidationGetCounters(t *testing.T) {
	svc := Validation(val.NewValidator())(&mockXTZService{})

	ctx := context.Background()
	tests := []struct {
		req   *service.GetCountersReq
		valid bool
	}{
		{
			req: &service.GetCountersReq{
				Network: "mainnet",
				Addresses: []string{
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe"},
			},
			valid: true,
		},
		{
			req: &service.GetCountersReq{
				Network: "wrongnetwork",
				Addresses: []string{
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe"},
			},
			valid: false,
		},
		{req: nil, valid: false},
		{req: &service.GetCountersReq{}, valid: false},
		{req: &service.GetCountersReq{Network: "mainnet"}, valid: false},
		{
			req: &service.GetCountersReq{
				Network: "wrongnetwork",
				Addresses: []string{
					"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756ccg", // invalid char
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe"},
			},
			valid: false,
		},
	}

	for _, test := range tests {
		_, err := svc.GetCounters(ctx, test.req)
		if test.valid {
			require.Nil(t, err)
		} else {
			require.NotNil(t, err)
		}
	}
}

func Test_XTZValidationGetTransactionsByHashes(t *testing.T) {
	svc := Validation(val.NewValidator())(&mockXTZService{})

	ctx := context.Background()
	tests := []struct {
		req   *service.GetTransactionsByHashesByCustomerReq
		valid bool
	}{
		{
			req: &service.GetTransactionsByHashesByCustomerReq{
				Network:    "mainnet",
				CustomerID: "daae03ef-fa60-4b22-9c10-552de333711a",
				Hashes: []string{
					"op5AGD3VrzgdzwTk7eNMGYEoQS6Zcsz6PWyYMk5kNvqSumDZReW",
					"ooXh2FstoqHnXD9Kqu7CVWtrs8VNVN2u3XyCnked7v38kjKVdyQ",
					"op3WBRzqfayJEbv7ApBkTBjHfqxRSoEwrpm16SjMn8wrUXiPjPc",
					"oohNnXjgArNuBSU3HQRkd9RDmaaUE9LpZne6qDdPfeV6ddNh971",
					"oocNzaGtRSa8VduCVYmSZBLqQvuPbV978aFtTNGPRbc3Y7Stbob"},
			},
			valid: true,
		},
		{
			req: &service.GetTransactionsByHashesByCustomerReq{
				Network:    "mainnet",
				CustomerID: "daae03ef-fa60-4b22-9c10-552de333711a",
				Hashes: []string{
					"onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ",
					"ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss",
					"opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU",
					"ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1",
					"ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2"},
			},
			valid: true,
		},
		{
			req: &service.GetTransactionsByHashesByCustomerReq{
				Network:    "mainnet",
				CustomerID: "", // missing customer ID
				Hashes: []string{
					"onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ",
					"ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss",
					"opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU",
					"ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1",
					"ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2"},
			},
			valid: false,
		},
		{
			req: &service.GetTransactionsByHashesByCustomerReq{
				Network:    "wrongnetwork",
				CustomerID: "daae03ef-fa60-4b22-9c10-552de333711a",
				Hashes: []string{
					"onu4xNr7NTUxGHPRGMQQrm5CD3CncDHLFvgNnHcWxkRn7QSdaDJ",
					"ooUsUE2SCwZCMwjmEgSSP8s3u2LhLtAG45j8hN2FHNEPLLULuss",
					"opPZPWPvvsKScmYxwSKTymxx8Gw7gjhjaCNU4QboZeuJ8vaiAnU",
					"ooJdn5tdzhC8YZSsKdphaYN1Jc7nS3GHNZ4eikz4Vzp1q6HS5d1",
					"ootwyMERpZfjAxgVbcBXfnLX5CeFPbKd13nMaP51G963gqkfrm2"},
			},
			valid: false,
		},
		{req: nil, valid: false},
		{req: &service.GetTransactionsByHashesByCustomerReq{}, valid: false},
		{req: &service.GetTransactionsByHashesByCustomerReq{Network: "mainnet"}, valid: false}}

	for _, test := range tests {
		_, _, err := svc.GetTransactionsByHashes(ctx, test.req)
		if test.valid {
			require.Nil(t, err)
		} else {
			require.NotNil(t, err)
		}
	}
}

func Test_XTZValidationGetTransactionsByDates(t *testing.T) {
	svc := Validation(val.NewValidator())(&mockXTZService{})

	ctx := context.Background()
	tests := []struct {
		req   *service.GetTransactionsByDatesByCustomerReq
		valid bool
	}{
		{
			req: &service.GetTransactionsByDatesByCustomerReq{
				Network:    "mainnet",
				CustomerID: "daae03ef-fa60-4b22-9c10-552de333711a",
				Addresses: []string{
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe",
				},
				FromDate: time.Date(2019, time.January, 1, 2, 3, 4, 0, time.UTC),
				ToDate:   time.Date(2019, time.January, 1, 2, 3, 4, 0, time.UTC),
				Limit:    100,
				Offset:   0,
			},
			valid: true,
		},
		{
			req: &service.GetTransactionsByDatesByCustomerReq{
				Network:    "mainnet",
				CustomerID: "", // missing customer ID
				Addresses: []string{
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe",
				},
				FromDate: time.Date(2019, time.January, 1, 2, 3, 4, 0, time.UTC),
				ToDate:   time.Date(2019, time.January, 1, 2, 3, 4, 0, time.UTC),
				Limit:    100,
				Offset:   0,
			},
			valid: false,
		},
		{
			req: &service.GetTransactionsByDatesByCustomerReq{
				Network:    "wrongnetwork",
				CustomerID: "daae03ef-fa60-4b22-9c10-552de333711a",
				Addresses: []string{
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe",
				},
				FromDate: time.Date(2019, time.January, 1, 2, 3, 4, 0, time.UTC),
				ToDate:   time.Date(2019, time.January, 1, 2, 3, 4, 0, time.UTC),
				Limit:    100,
				Offset:   0,
			},
			valid: false,
		},
		{req: nil, valid: false},
		{req: &service.GetTransactionsByDatesByCustomerReq{}, valid: false},
		{req: &service.GetTransactionsByDatesByCustomerReq{Network: "mainnet"}, valid: false},
		{
			req: &service.GetTransactionsByDatesByCustomerReq{
				Network:    "mainnet",
				CustomerID: "daae03ef-fa60-4b22-9c10-552de333711a",
				Addresses: []string{
					"0xdac17f958d2ee523a2206206994597c13d831ecz", // wrong char
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe",
				},
				FromDate: time.Date(2019, time.January, 1, 2, 3, 4, 0, time.UTC),
				ToDate:   time.Date(2019, time.January, 1, 2, 3, 4, 0, time.UTC),
				Limit:    100,
				Offset:   0,
			},
			valid: false,
		},
		{
			req: &service.GetTransactionsByDatesByCustomerReq{
				Network:    "mainnet",
				CustomerID: "daae03ef-fa60-4b22-9c10-552de333711a",
				Addresses: []string{
					"tz1SYq214SCBy9naR6cvycQsYcUGpBqQAE8d",
					"tz1bY8g2N558B2SoyriM5WeGsXSWtaf6qHP2",
					"tz1ihCKcZ8iRxK1NX35u5xXvGRvnDVCvfPu1",
					"tz1MTRbdWuVQh4ZyYnSkp7x2t8oXQHFHu9nR",
					"tz1MXjdb684ByEP5qUn5J7EMub7Sr8eBziDe",
				},
				FromDate: time.Date(2019, time.January, 1, 2, 3, 4, 0, time.UTC),
				ToDate:   time.Date(2019, time.January, 1, 1, 3, 4, 0, time.UTC), // from date is after to dae
				Limit:    100,
				Offset:   0,
			},
			valid: false,
		},
	}

	for _, test := range tests {
		_, _, _, err := svc.GetTransactionsByDates(ctx, test.req)
		if test.valid {
			require.Nil(t, err)
		} else {
			require.NotNil(t, err)
		}
	}
}

type mockXTZService struct{}

func (m *mockXTZService) AddAddresses(ctx context.Context, req *service.AddAddressesReq) error {
	return nil
}
func (m *mockXTZService) Broadcast(ctx context.Context, req *service.BroadcastByCustomerReq) (string, error) {
	return "", nil
}
func (m *mockXTZService) GetBalances(ctx context.Context, req *service.GetBalancesReq) ([]*model.Balance, error) {
	return nil, nil
}
func (m *mockXTZService) GetBlockchainInfo(ctx context.Context, req *service.GetBlockchainInfoReq) (*model.BlockchainInfo, error) {
	return nil, nil
}
func (m *mockXTZService) GetEstimatedFee(ctx context.Context, req *service.GetEstimatedFeeReq) (*model.Fees, error) {
	return nil, nil
}
func (m *mockXTZService) GetCounters(ctx context.Context, req *service.GetCountersReq) ([]*model.Counter, error) {
	return nil, nil
}
func (m *mockXTZService) GetTransactionsByHashes(ctx context.Context, req *service.GetTransactionsByHashesByCustomerReq) ([]*model.Transaction, uint64, error) {
	return nil, 0, nil
}
func (m *mockXTZService) GetTransactionsByBlocks(ctx context.Context, req *service.GetTransactionsByBlocksByCustomerReq) ([]*model.Transaction, uint64, uint64, error) {
	return nil, 0, 0, nil
}
func (m *mockXTZService) GetTransactionsByDates(ctx context.Context, req *service.GetTransactionsByDatesByCustomerReq) ([]*model.Transaction, uint64, uint64, error) {
	return nil, 0, 0, nil
}
func (m *mockXTZService) GetTransactionsByAttributes(ctx context.Context, req *service.GetTransactionsByAttributesByCustomerReq) ([]*model.Transaction, uint64, error) {
	return nil, 0, nil
}
