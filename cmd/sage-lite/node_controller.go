package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cometbft/cometbft/config"
	cmtcrypto "github.com/cometbft/cometbft/crypto/ed25519"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
	"github.com/rs/zerolog"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/orchestrator"
)

// SageNodeController implements orchestrator.NodeController for the CometBFT node lifecycle.
// The ABCI app is REUSED across stop/start cycles (it holds open SQLite/BadgerDB connections).
// Only the CometBFT node wrapper is recreated — stopped CometBFT nodes cannot be restarted.
type SageNodeController struct {
	mu        sync.Mutex
	cometNode *node.Node
	cometCfg  *config.Config
	app       *sageabci.SageApp
	pv        *privval.FilePV
	nodeKey   *p2p.NodeKey
	cmtLogger cmtlog.Logger
	logger    zerolog.Logger
	dataDir   string
	running   bool
}

// Ensure SageNodeController implements orchestrator.NodeController at compile time.
var _ orchestrator.NodeController = (*SageNodeController)(nil)

// NewSageNodeController creates a new node controller.
func NewSageNodeController(
	cometCfg *config.Config,
	app *sageabci.SageApp,
	pv *privval.FilePV,
	nodeKey *p2p.NodeKey,
	cmtLogger cmtlog.Logger,
	logger zerolog.Logger,
	dataDir string,
) *SageNodeController {
	return &SageNodeController{
		cometCfg:  cometCfg,
		app:       app,
		pv:        pv,
		nodeKey:   nodeKey,
		cmtLogger: cmtLogger,
		logger:    logger,
		dataDir:   dataDir,
	}
}

// StopChain stops the CometBFT node. Idempotent — safe to call if already stopped.
func (c *SageNodeController) StopChain() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running || c.cometNode == nil {
		c.logger.Info().Msg("StopChain: node already stopped")
		return nil
	}

	c.logger.Info().Msg("stopping CometBFT node for redeployment")
	if err := c.cometNode.Stop(); err != nil {
		return fmt.Errorf("stop CometBFT: %w", err)
	}
	c.cometNode.Wait()
	c.running = false
	c.logger.Info().Msg("CometBFT node stopped")
	return nil
}

// StartChain creates a NEW CometBFT node and starts it.
// A stopped CometBFT node cannot be restarted — a fresh node.NewNode must be created.
func (c *SageNodeController) StartChain() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("chain is already running")
	}

	c.logger.Info().Msg("creating fresh CometBFT node")

	// Reload FilePV to pick up any state resets
	c.pv = privval.LoadFilePV(
		c.cometCfg.PrivValidatorKeyFile(),
		c.cometCfg.PrivValidatorStateFile(),
	)

	cometNode, err := node.NewNode(
		c.cometCfg,
		c.pv,
		c.nodeKey,
		proxy.NewLocalClientCreator(c.app),
		node.DefaultGenesisDocProviderFunc(c.cometCfg),
		config.DefaultDBProvider,
		node.DefaultMetricsProvider(c.cometCfg.Instrumentation),
		c.cmtLogger,
	)
	if err != nil {
		return fmt.Errorf("create CometBFT node: %w", err)
	}

	if err := cometNode.Start(); err != nil {
		return fmt.Errorf("start CometBFT: %w", err)
	}

	c.cometNode = cometNode
	c.running = true
	c.logger.Info().
		Str("node_id", string(cometNode.NodeInfo().ID())).
		Msg("CometBFT node started")

	return nil
}

// RegenerateGenesis reads the existing genesis, rebuilds it with the new validator set,
// and writes it back. Preserves ChainID and ConsensusParams. Sets InitialHeight=1.
func (c *SageNodeController) RegenerateGenesis(validators []orchestrator.ValidatorInfo) error {
	genesisPath := filepath.Join(c.cometCfg.RootDir, "config", "genesis.json")

	// Read existing genesis to preserve ChainID and ConsensusParams
	genData, err := os.ReadFile(genesisPath)
	if err != nil {
		return fmt.Errorf("read genesis: %w", err)
	}

	var existingGen cmttypes.GenesisDoc
	if err := json.Unmarshal(genData, &existingGen); err != nil {
		return fmt.Errorf("parse genesis: %w", err)
	}

	c.logger.Info().
		Int("validator_count", len(validators)).
		Str("chain_id", existingGen.ChainID).
		Msg("regenerating genesis with new validator set")

	// Build new validator list
	genValidators := make([]cmttypes.GenesisValidator, 0, len(validators))

	for _, v := range validators {
		var pubKey cmtcrypto.PubKey
		if len(v.PubKey) == cmtcrypto.PubKeySize {
			pubKey = cmtcrypto.PubKey(v.PubKey)
		} else {
			// If no pubkey provided, use the local validator key (for the primary node)
			localPK, ok := c.pv.Key.PubKey.(cmtcrypto.PubKey)
			if !ok {
				return fmt.Errorf("local validator pubkey is not ed25519")
			}
			pubKey = localPK
		}

		power := v.Power
		if power <= 0 {
			power = 10
		}

		genValidators = append(genValidators, cmttypes.GenesisValidator{
			Address: pubKey.Address(),
			PubKey:  pubKey,
			Power:   power,
			Name:    v.Name,
		})
	}

	// If no validators provided, fall back to the local validator
	if len(genValidators) == 0 {
		genValidators = append(genValidators, cmttypes.GenesisValidator{
			Address: c.pv.Key.PubKey.Address(),
			PubKey:  c.pv.Key.PubKey,
			Power:   10,
			Name:    "personal",
		})
	}

	newGen := cmttypes.GenesisDoc{
		ChainID:         existingGen.ChainID,
		GenesisTime:     cmttime.Now(),
		ConsensusParams: existingGen.ConsensusParams,
		InitialHeight:   1,
		Validators:      genValidators,
	}

	if err := newGen.ValidateAndComplete(); err != nil {
		return fmt.Errorf("validate new genesis: %w", err)
	}

	if err := newGen.SaveAs(genesisPath); err != nil {
		return fmt.Errorf("save new genesis: %w", err)
	}

	c.logger.Info().
		Int("validators", len(genValidators)).
		Msg("genesis regenerated")
	return nil
}

// WipeChainState removes CometBFT data DBs and resets BadgerDB.
// This follows the exact pattern from migrate.go lines 76-103.
// The priv_validator_state.json is reset to height 0 so CometBFT
// accepts signing at the new genesis height.
func (c *SageNodeController) WipeChainState() error {
	c.logger.Info().Msg("wiping chain state for redeployment")

	badgerPath := filepath.Join(c.dataDir, "badger")

	// Step 1: Wipe BadgerDB (on-chain state — will be rebuilt)
	if _, statErr := os.Stat(badgerPath); statErr == nil {
		if removeErr := os.RemoveAll(badgerPath); removeErr != nil {
			return fmt.Errorf("remove badger: %w", removeErr)
		}
		if mkErr := os.MkdirAll(badgerPath, 0700); mkErr != nil {
			return fmt.Errorf("recreate badger dir: %w", mkErr)
		}
		c.logger.Info().Msg("reset BadgerDB")
	}

	// Step 2: Wipe CometBFT data DBs (blocks/state — incompatible with new chain)
	cometDataDir := filepath.Join(c.cometCfg.RootDir, "data")
	if _, statErr := os.Stat(cometDataDir); statErr == nil {
		for _, dbName := range []string{"blockstore.db", "state.db", "tx_index.db", "evidence.db"} {
			dbPath := filepath.Join(cometDataDir, dbName)
			if removeErr := os.RemoveAll(dbPath); removeErr != nil {
				c.logger.Warn().Err(removeErr).Str("db", dbName).Msg("could not remove CometBFT DB")
			}
		}

		// Reset validator state to height 0 — CRITICAL: CometBFT refuses to sign at
		// heights lower than the last signed height. Must reset before new genesis.
		pvStatePath := filepath.Join(cometDataDir, "priv_validator_state.json")
		pvState := []byte(`{"height":"0","round":0,"step":0}`)
		if writeErr := os.WriteFile(pvStatePath, pvState, 0600); writeErr != nil {
			return fmt.Errorf("reset validator state: %w", writeErr)
		}
		c.logger.Info().Msg("reset CometBFT data and validator state")
	}

	return nil
}

// GetDataDir returns the SAGE data directory.
func (c *SageNodeController) GetDataDir() string {
	return c.dataDir
}

// IsRunning returns whether the CometBFT node is currently running.
func (c *SageNodeController) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

// GetCometNode returns the current CometBFT node (may be nil if stopped).
func (c *SageNodeController) GetCometNode() *node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cometNode
}
