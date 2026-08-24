package ethclient_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/params"
)

// TestSubscribeBlockLogsE2E starts a real node, subscribes over websocket, and
// checks that each notification is the complete set of matching logs for one block.
func TestSubscribeBlockLogsE2E(t *testing.T) {
	t.Parallel()

	var (
		logger = common.HexToAddress("0x1000")
		topic0 = common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
		topic1 = common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
		code   = loggerBytecode(topic0, topic1)
	)

	genesis := &core.Genesis{
		Config: params.AllEthashProtocolChanges,
		Alloc: types.GenesisAlloc{
			testAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			logger:   {Nonce: 1, Code: code},
		},
		BaseFee:   big.NewInt(params.InitialBaseFee),
		GasLimit:  8_000_000,
		Timestamp: 9000,
	}

	stack, ethservice := startBlockLogsNode(t, genesis)
	defer stack.Close()

	ec, err := ethclient.Dial(stack.WSEndpoint())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer ec.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ch := make(chan []*types.Log, 8)
	sub, err := ec.SubscribeBlockLogs(ctx, ethereum.FilterQuery{Addresses: []common.Address{logger}}, ch)
	if err != nil {
		t.Fatalf("subscribe blockLogs: %v", err)
	}
	defer sub.Unsubscribe()

	signer := types.LatestSigner(genesis.Config)
	_, blocks, _ := core.GenerateChainWithGenesis(genesis, ethash.NewFaker(), 2, func(i int, gen *core.BlockGen) {
		tx, err := types.SignTx(types.NewTransaction(uint64(i), logger, big.NewInt(0), 50_000, gen.BaseFee(), nil), signer, testKey)
		if err != nil {
			t.Fatalf("sign tx: %v", err)
		}
		gen.AddTx(tx)
	})
	if _, err := ethservice.BlockChain().InsertChain(blocks); err != nil {
		t.Fatalf("insert chain: %v", err)
	}

	var got [][]*types.Log
	deadline := time.After(10 * time.Second)
	for len(got) < 2 {
		select {
		case batch := <-ch:
			got = append(got, batch)
		case err := <-sub.Err():
			t.Fatalf("subscription error: %v", err)
		case <-deadline:
			t.Fatalf("timeout waiting for blockLogs notifications, got %d", len(got))
		}
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 websocket notifications, got %d", len(got))
	}
	for i, batch := range got {
		if len(batch) != 2 {
			t.Fatalf("notification %d: expected 2 matching logs for the block, got %d", i, len(batch))
		}
		blockHash := batch[0].BlockHash
		blockNumber := batch[0].BlockNumber
		if blockHash != blocks[i].Hash() {
			t.Fatalf("notification %d: block hash %x, want %x", i, blockHash, blocks[i].Hash())
		}
		if blockNumber != blocks[i].NumberU64() {
			t.Fatalf("notification %d: block number %d, want %d", i, blockNumber, blocks[i].NumberU64())
		}
		for j, log := range batch {
			if log.Address != logger {
				t.Fatalf("notification %d log %d: unexpected address %x", i, j, log.Address)
			}
			if log.BlockHash != blockHash || log.BlockNumber != blockNumber {
				t.Fatalf("notification %d: logs split across blocks: %x/#%d and %x/#%d", i, blockHash, blockNumber, log.BlockHash, log.BlockNumber)
			}
			if log.Removed {
				t.Fatalf("notification %d log %d: unexpected removed log", i, j)
			}
		}
		if batch[0].Topics[0] != topic0 || batch[1].Topics[0] != topic1 {
			t.Fatalf("notification %d: expected topics in contract order, got %x then %x", i, batch[0].Topics[0], batch[1].Topics[0])
		}
	}

	select {
	case extra := <-ch:
		t.Fatalf("unexpected extra notification with %d logs", len(extra))
	case <-time.After(300 * time.Millisecond):
	}
}

func startBlockLogsNode(t *testing.T, genesis *core.Genesis) (*node.Node, *eth.Ethereum) {
	t.Helper()

	nodeConf := node.DefaultConfig
	nodeConf.DataDir = ""
	nodeConf.P2P = p2p.Config{NoDiscovery: true, ListenAddr: "127.0.0.1:0"}
	nodeConf.WSHost = "127.0.0.1"
	nodeConf.WSPort = 0
	nodeConf.WSModules = []string{"eth"}

	stack, err := node.New(&nodeConf)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	ethConf := ethconfig.Defaults
	ethConf.Genesis = genesis
	ethConf.NetworkId = genesis.Config.ChainID.Uint64()
	ethConf.SyncMode = ethconfig.FullSync
	ethConf.RPCGasCap = 1_000_000

	ethservice, err := eth.New(stack, &ethConf)
	if err != nil {
		stack.Close()
		t.Fatalf("new ethereum service: %v", err)
	}
	if err := stack.Start(); err != nil {
		stack.Close()
		t.Fatalf("start node: %v", err)
	}
	return stack, ethservice
}

func loggerBytecode(topic0, topic1 common.Hash) []byte {
	var code []byte
	appendLog1 := func(topic common.Hash) {
		// LOG1 pops offset, size, then topic. Push in reverse.
		code = append(code, 0x7f)
		code = append(code, topic.Bytes()...)
		code = append(code, 0x60, 0x00, 0x60, 0x00, 0xa1)
	}
	appendLog1(topic0)
	appendLog1(topic1)
	return append(code, 0x00)
}

func TestSubscribeBlockLogsE2E_TopicFilter(t *testing.T) {
	t.Parallel()

	var (
		logger = common.HexToAddress("0x1000")
		topic0 = common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
		topic1 = common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	)

	genesis := &core.Genesis{
		Config: params.AllEthashProtocolChanges,
		Alloc: types.GenesisAlloc{
			testAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			logger:   {Nonce: 1, Code: loggerBytecode(topic0, topic1)},
		},
		BaseFee:   big.NewInt(params.InitialBaseFee),
		GasLimit:  8_000_000,
		Timestamp: 9000,
	}

	stack, ethservice := startBlockLogsNode(t, genesis)
	defer stack.Close()

	ec, err := ethclient.Dial(stack.WSEndpoint())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer ec.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ch := make(chan []*types.Log, 8)
	sub, err := ec.SubscribeBlockLogs(ctx, ethereum.FilterQuery{
		Addresses: []common.Address{logger},
		Topics:    [][]common.Hash{{topic0}},
	}, ch)
	if err != nil {
		t.Fatalf("subscribe blockLogs: %v", err)
	}
	defer sub.Unsubscribe()

	signer := types.LatestSigner(genesis.Config)
	_, blocks, _ := core.GenerateChainWithGenesis(genesis, ethash.NewFaker(), 1, func(i int, gen *core.BlockGen) {
		tx, err := types.SignTx(types.NewTransaction(uint64(i), logger, big.NewInt(0), 50_000, gen.BaseFee(), nil), signer, testKey)
		if err != nil {
			t.Fatalf("sign tx: %v", err)
		}
		gen.AddTx(tx)
	})
	if _, err := ethservice.BlockChain().InsertChain(blocks); err != nil {
		t.Fatalf("insert chain: %v", err)
	}

	select {
	case batch := <-ch:
		if len(batch) != 1 {
			t.Fatalf("expected only the matching log from the block, got %d", len(batch))
		}
		if batch[0].Topics[0] != topic0 {
			t.Fatalf("expected topic0, got %x", batch[0].Topics[0])
		}
		if batch[0].BlockHash != blocks[0].Hash() {
			t.Fatalf("unexpected block hash %x", batch[0].BlockHash)
		}
	case err := <-sub.Err():
		t.Fatalf("subscription error: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for filtered blockLogs notification")
	}
}

// TestSubscribeBlockLogsE2E_MoreThan512Logs checks a single block with more matching
// logs than the chain-layer reorg flush threshold (512). The expected result is
// still one websocket notification containing every matching log from that block.
func TestSubscribeBlockLogsE2E_MoreThan512Logs(t *testing.T) {
	t.Parallel()

	const logCount = 513
	logger := common.HexToAddress("0x1000")

	genesis := &core.Genesis{
		Config: params.AllEthashProtocolChanges,
		Alloc: types.GenesisAlloc{
			testAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			logger:   {Nonce: 1, Code: repeatLog0Bytecode(logCount)},
		},
		BaseFee:   big.NewInt(params.InitialBaseFee),
		GasLimit:  8_000_000,
		Timestamp: 9000,
	}

	stack, ethservice := startBlockLogsNode(t, genesis)
	defer stack.Close()

	ec, err := ethclient.Dial(stack.WSEndpoint())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer ec.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ch := make(chan []*types.Log, 8)
	sub, err := ec.SubscribeBlockLogs(ctx, ethereum.FilterQuery{Addresses: []common.Address{logger}}, ch)
	if err != nil {
		t.Fatalf("subscribe blockLogs: %v", err)
	}
	defer sub.Unsubscribe()

	signer := types.LatestSigner(genesis.Config)
	_, blocks, _ := core.GenerateChainWithGenesis(genesis, ethash.NewFaker(), 1, func(i int, gen *core.BlockGen) {
		tx, err := types.SignTx(types.NewTransaction(uint64(i), logger, big.NewInt(0), 1_000_000, gen.BaseFee(), nil), signer, testKey)
		if err != nil {
			t.Fatalf("sign tx: %v", err)
		}
		gen.AddTx(tx)
	})
	if _, err := ethservice.BlockChain().InsertChain(blocks); err != nil {
		t.Fatalf("insert chain: %v", err)
	}

	var batches [][]*types.Log
	deadline := time.After(10 * time.Second)
	for len(batches) == 0 {
		select {
		case batch := <-ch:
			batches = append(batches, batch)
		case err := <-sub.Err():
			t.Fatalf("subscription error: %v", err)
		case <-deadline:
			t.Fatal("timeout waiting for blockLogs notification")
		}
	}
	// Drain any follow-up notifications that would indicate the block was split.
	drain := time.After(500 * time.Millisecond)
drainLoop:
	for {
		select {
		case batch := <-ch:
			batches = append(batches, batch)
		case <-drain:
			break drainLoop
		}
	}

	if len(batches) != 1 {
		counts := make([]int, len(batches))
		for i, batch := range batches {
			counts[i] = len(batch)
		}
		t.Fatalf("expected 1 websocket notification for the block, got %d notifications with log counts %v", len(batches), counts)
	}
	batch := batches[0]
	if len(batch) != logCount {
		t.Fatalf("expected all %d matching logs in one notification, got %d", logCount, len(batch))
	}
	blockHash := blocks[0].Hash()
	for i, log := range batch {
		if log.Address != logger {
			t.Fatalf("log %d: unexpected address %x", i, log.Address)
		}
		if log.BlockHash != blockHash {
			t.Fatalf("log %d: block hash %x, want %x", i, log.BlockHash, blockHash)
		}
		if log.Index != uint(i) {
			t.Fatalf("log %d: logIndex %d, want %d", i, log.Index, i)
		}
	}
}

func repeatLog0Bytecode(n int) []byte {
	if n <= 0 || n > 0xffff {
		panic("log count out of range")
	}
	end := byte(23)
	return []byte{
		0x61, byte(n >> 8), byte(n), // PUSH2 n
		0x5b,            // JUMPDEST loop
		0x80,            // DUP1
		0x15,            // ISZERO
		0x61, 0x00, end, // PUSH2 end
		0x57,       // JUMPI
		0x60, 0x00, // PUSH1 0  (size)
		0x60, 0x00, // PUSH1 0  (offset)
		0xa0,       // LOG0
		0x60, 0x01, // PUSH1 1
		0x90,             // SWAP1
		0x03,             // SUB
		0x61, 0x00, 0x03, // PUSH2 loop
		0x56, // JUMP
		0x5b, // JUMPDEST end
		0x00, // STOP
	}
}
