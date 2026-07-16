package abci

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	// These consensus bounds keep attacker-controlled strings from multiplying
	// into oversized Badger keys across a 100-validator stats update. They are
	// activated only for a block routed through the app-v20 atomic ceremony/path.
	maxAppV20IdentifierBytes    = 512
	maxAppV20MetadataBytes      = 64 << 10
	maxAppV20ContentBytes       = 512 << 10
	maxAppV20EncodedRecordBytes = 64 << 10
	maxAppV20CollectionItems    = 64
	maxAppV20Validators         = 100
)

const appV20ResourceLimitCode uint32 = 111

func appV20Identifier(field, value string) error {
	if len(value) > maxAppV20IdentifierBytes {
		return fmt.Errorf("%s has %d bytes, limit %d", field, len(value), maxAppV20IdentifierBytes)
	}
	return nil
}

func appV20Identifiers(values ...struct {
	field string
	value string
}) error {
	for _, candidate := range values {
		if err := appV20Identifier(candidate.field, candidate.value); err != nil {
			return err
		}
	}
	return nil
}

func appV20Metadata(field, value string) error {
	if len(value) > maxAppV20MetadataBytes {
		return fmt.Errorf("%s has %d bytes, limit %d", field, len(value), maxAppV20MetadataBytes)
	}
	return nil
}

func appV20ExactBytes(field string, value []byte, size int, optional bool) error {
	if optional && len(value) == 0 {
		return nil
	}
	if len(value) != size {
		return fmt.Errorf("%s has %d bytes, want %d", field, len(value), size)
	}
	return nil
}

func appV20StringCollection(field string, values []string) error {
	if len(values) > maxAppV20CollectionItems {
		return fmt.Errorf("%s has %d entries, limit %d", field, len(values), maxAppV20CollectionItems)
	}
	for i, value := range values {
		if err := appV20Identifier(fmt.Sprintf("%s[%d]", field, i), value); err != nil {
			return err
		}
	}
	return nil
}

func appV20EncodedRecord(field string, size int) error {
	if size > maxAppV20EncodedRecordBytes {
		return fmt.Errorf("%s encodes to at least %d bytes, limit %d", field, size, maxAppV20EncodedRecordBytes)
	}
	return nil
}

func validateAppV20AgentRecord(agent *store.OnChainAgent) error {
	encoded, err := json.Marshal(agent)
	if err != nil {
		return fmt.Errorf("encode agent record: %w", err)
	}
	return appV20EncodedRecord("agent", len(encoded))
}

func appV20StringsSize(values []string) int {
	size := 4
	for _, value := range values {
		size += 4 + len(value)
	}
	return size
}

func id(field, value string) struct {
	field string
	value string
} {
	return struct {
		field string
		value string
	}{field: field, value: value}
}

// validateAppV20TxResources bounds every payload field that can become a
// consensus key, be duplicated into several consensus values, or carry a large
// metadata allocation. The block's independent 1 MiB raw-byte cap remains the
// aggregate bound. Validation runs before proof claims, nonces, or handlers.
func validateAppV20TxResources(parsed *tx.ParsedTx) error { //nolint:gocyclo // explicit wire-family audit is safer than reflection
	if parsed == nil {
		return nil
	}
	if len(parsed.AgentRequest) > maxAppV20ContentBytes {
		return fmt.Errorf("agent request has %d bytes, limit %d", len(parsed.AgentRequest), maxAppV20ContentBytes)
	}
	if len(parsed.AgentNonce) > maxAppV20IdentifierBytes {
		return fmt.Errorf("agent nonce has %d bytes, limit %d", len(parsed.AgentNonce), maxAppV20IdentifierBytes)
	}

	switch parsed.Type {
	case tx.TxTypeMemorySubmit:
		p := parsed.MemorySubmit
		if p == nil {
			return nil
		}
		if err := appV20Identifiers(id("memory id", p.MemoryID), id("domain", p.DomainTag), id("parent hash", p.ParentHash), id("task status", p.TaskStatus)); err != nil {
			return err
		}
		// Empty is a canonical request for processMemorySubmit to derive the
		// SHA-256 from Content. A supplied hash must have the exact digest size.
		if err := appV20ExactBytes("content hash", p.ContentHash, sha256.Size, true); err != nil {
			return err
		}
		if err := appV20ExactBytes("embedding hash", p.EmbeddingHash, sha256.Size, true); err != nil {
			return err
		}
		if len(p.Content) > maxAppV20ContentBytes {
			return fmt.Errorf("memory content has %d bytes, limit %d", len(p.Content), maxAppV20ContentBytes)
		}
	case tx.TxTypeMemoryVote:
		if p := parsed.MemoryVote; p != nil {
			if err := appV20Identifier("memory id", p.MemoryID); err != nil {
				return err
			}
			return appV20Metadata("vote rationale", p.Rationale)
		}
	case tx.TxTypeMemoryChallenge:
		if p := parsed.MemoryChallenge; p != nil {
			if err := appV20Identifier("memory id", p.MemoryID); err != nil {
				return err
			}
			if err := appV20Metadata("challenge reason", p.Reason); err != nil {
				return err
			}
			return appV20Metadata("challenge evidence", p.Evidence)
		}
	case tx.TxTypeMemoryCorroborate:
		if p := parsed.MemoryCorroborate; p != nil {
			if err := appV20Identifier("memory id", p.MemoryID); err != nil {
				return err
			}
			return appV20Metadata("corroboration evidence", p.Evidence)
		}
	case tx.TxTypeMemoryReinstate:
		if p := parsed.MemoryReinstate; p != nil {
			if err := appV20Identifier("memory id", p.MemoryID); err != nil {
				return err
			}
			return appV20Metadata("reinstate reason", p.Reason)
		}
	case tx.TxTypeAccessRequest:
		if p := parsed.AccessRequest; p != nil {
			if err := appV20Identifiers(id("requester id", p.RequesterID), id("target domain", p.TargetDomain)); err != nil {
				return err
			}
			if err := appV20Metadata("access justification", p.Justification); err != nil {
				return err
			}
			return appV20EncodedRecord("access request", 4+len(p.RequesterID)+4+len(p.TargetDomain)+4+len(p.Justification)+16)
		}
	case tx.TxTypeAccessGrant:
		if p := parsed.AccessGrant; p != nil {
			return appV20Identifiers(id("granter id", p.GranterID), id("grantee id", p.GranteeID), id("domain", p.Domain), id("request id", p.RequestID), id("expected owner id", p.ExpectedOwnerID), id("expected owned domain", p.ExpectedOwnedDomain))
		}
	case tx.TxTypeAccessRevoke:
		if p := parsed.AccessRevoke; p != nil {
			if err := appV20Identifiers(id("revoker id", p.RevokerID), id("grantee id", p.GranteeID), id("domain", p.Domain), id("expected owner id", p.ExpectedOwnerID), id("expected owned domain", p.ExpectedOwnedDomain)); err != nil {
				return err
			}
			return appV20Metadata("revoke reason", p.Reason)
		}
	case tx.TxTypeAccessQuery:
		if p := parsed.AccessQuery; p != nil {
			return appV20Identifiers(id("agent id", p.AgentID), id("domain", p.Domain))
		}
	case tx.TxTypeDomainRegister:
		if p := parsed.DomainRegister; p != nil {
			if err := appV20Identifiers(id("domain", p.DomainName), id("owner agent id", p.OwnerAgentID), id("parent domain", p.ParentDomain)); err != nil {
				return err
			}
			return appV20Metadata("domain description", p.Description)
		}
	case tx.TxTypeOrgRegister:
		if p := parsed.OrgRegister; p != nil {
			if err := appV20Identifiers(id("org id", p.OrgID), id("org name", p.Name), id("admin agent id", p.AdminAgent)); err != nil {
				return err
			}
			if err := appV20Metadata("org description", p.Description); err != nil {
				return err
			}
			return appV20EncodedRecord("organization", 4+len(p.Name)+4+len(p.Description)+4+len(p.AdminAgent)+8)
		}
	case tx.TxTypeOrgAddMember:
		if p := parsed.OrgAddMember; p != nil {
			return appV20Identifiers(id("org id", p.OrgID), id("agent id", p.AgentID), id("role", p.Role))
		}
	case tx.TxTypeOrgRemoveMember:
		if p := parsed.OrgRemoveMember; p != nil {
			if err := appV20Identifiers(id("org id", p.OrgID), id("agent id", p.AgentID)); err != nil {
				return err
			}
			return appV20Metadata("remove reason", p.Reason)
		}
	case tx.TxTypeOrgSetClearance:
		if p := parsed.OrgSetClearance; p != nil {
			return appV20Identifiers(id("org id", p.OrgID), id("agent id", p.AgentID))
		}
	case tx.TxTypeFederationPropose:
		if p := parsed.FederationPropose; p != nil {
			if err := appV20Identifiers(id("proposer org id", p.ProposerOrgID), id("target org id", p.TargetOrgID)); err != nil {
				return err
			}
			if err := appV20StringCollection("allowed domains", p.AllowedDomains); err != nil {
				return err
			}
			if err := appV20StringCollection("allowed departments", p.AllowedDepts); err != nil {
				return err
			}
			return appV20EncodedRecord("federation", 4+len(p.ProposerOrgID)+4+len(p.TargetOrgID)+16+appV20StringsSize(p.AllowedDomains)+appV20StringsSize(p.AllowedDepts))
		}
	case tx.TxTypeFederationApprove:
		if p := parsed.FederationApprove; p != nil {
			return appV20Identifiers(id("federation id", p.FederationID), id("approver org id", p.ApproverOrgID))
		}
	case tx.TxTypeFederationRevoke:
		if p := parsed.FederationRevoke; p != nil {
			if err := appV20Identifiers(id("federation id", p.FederationID), id("revoker org id", p.RevokerOrgID)); err != nil {
				return err
			}
			return appV20Metadata("federation revoke reason", p.Reason)
		}
	case tx.TxTypeDeptRegister:
		if p := parsed.DeptRegister; p != nil {
			if err := appV20Identifiers(id("org id", p.OrgID), id("department id", p.DeptID), id("department name", p.DeptName), id("parent department", p.ParentDept)); err != nil {
				return err
			}
			if err := appV20Metadata("department description", p.Description); err != nil {
				return err
			}
			return appV20EncodedRecord("department", 4+len(p.DeptName)+4+len(p.Description)+4+len(p.ParentDept)+8)
		}
	case tx.TxTypeDeptAddMember:
		if p := parsed.DeptAddMember; p != nil {
			return appV20Identifiers(id("org id", p.OrgID), id("department id", p.DeptID), id("agent id", p.AgentID), id("role", p.Role))
		}
	case tx.TxTypeDeptRemoveMember:
		if p := parsed.DeptRemoveMember; p != nil {
			if err := appV20Identifiers(id("org id", p.OrgID), id("department id", p.DeptID), id("agent id", p.AgentID)); err != nil {
				return err
			}
			return appV20Metadata("department remove reason", p.Reason)
		}
	case tx.TxTypeAgentRegister:
		if p := parsed.AgentRegister; p != nil {
			if err := appV20Identifiers(id("agent id", p.AgentID), id("agent name", p.Name), id("role", p.Role)); err != nil {
				return err
			}
			if err := appV20Metadata("agent bio", p.BootBio); err != nil {
				return err
			}
			if err := appV20Identifier("provider", p.Provider); err != nil {
				return err
			}
			if err := appV20Metadata("p2p address", p.P2PAddress); err != nil {
				return err
			}
			// OnChainAgent is JSON; six is the maximum expansion of one input
			// byte as a JSON \u00XX escape. This conservative bound guarantees
			// the stored record remains below the encoded-record ceiling.
			return appV20EncodedRecord("agent", 256+6*(len(p.AgentID)+len(p.Name)+len(p.Role)+len(p.BootBio)+len(p.Provider)+len(p.P2PAddress)))
		}
	case tx.TxTypeAgentUpdate:
		if p := parsed.AgentUpdateTx; p != nil {
			if err := appV20Identifiers(id("agent id", p.AgentID), id("agent name", p.Name)); err != nil {
				return err
			}
			return appV20Metadata("agent bio", p.BootBio)
		}
	case tx.TxTypeAgentSetPermission:
		if p := parsed.AgentSetPermission; p != nil {
			if err := appV20Identifiers(id("agent id", p.AgentID), id("org id", p.OrgID), id("department id", p.DeptID)); err != nil {
				return err
			}
			if err := appV20Metadata("domain access", p.DomainAccess); err != nil {
				return err
			}
			return appV20Metadata("visible agents", p.VisibleAgents)
		}
	case tx.TxTypeMemoryReassign:
		if p := parsed.MemoryReassign; p != nil {
			return appV20Identifiers(id("source agent id", p.SourceAgentID), id("target agent id", p.TargetAgentID))
		}
	case tx.TxTypeGovPropose:
		if p := parsed.GovPropose; p != nil {
			if err := appV20Identifier("governance target id", p.TargetID); err != nil {
				return err
			}
			if err := appV20ExactBytes("governance target public key", p.TargetPubKey, ed25519.PublicKeySize, true); err != nil {
				return err
			}
			if err := appV20Metadata("governance reason", p.Reason); err != nil {
				return err
			}
			if len(p.Payload) > maxAppV20ContentBytes {
				return fmt.Errorf("governance payload has %d bytes, limit %d", len(p.Payload), maxAppV20ContentBytes)
			}
		}
	case tx.TxTypeGovVote:
		if p := parsed.GovVote; p != nil {
			return appV20Identifier("proposal id", p.ProposalID)
		}
	case tx.TxTypeGovCancel:
		if p := parsed.GovCancel; p != nil {
			return appV20Identifier("proposal id", p.ProposalID)
		}
	case tx.TxTypeUpgradePropose:
		if p := parsed.UpgradePropose; p != nil {
			return appV20Identifiers(id("upgrade name", p.Name), id("binary sha256", p.BinarySHA256), id("proposer id", p.ProposerID), id("governance domain", p.GovernanceDomain))
		}
	case tx.TxTypeUpgradeCancel:
		if p := parsed.UpgradeCancel; p != nil {
			if err := appV20Identifiers(id("upgrade name", p.Name), id("canceller id", p.CancellerID)); err != nil {
				return err
			}
			return appV20Metadata("upgrade cancel reason", p.Reason)
		}
	case tx.TxTypeUpgradeRevert:
		if p := parsed.UpgradeRevert; p != nil {
			return appV20Identifiers(id("upgrade name", p.Name), id("proposer id", p.ProposerID))
		}
	case tx.TxTypeDomainReassign:
		if p := parsed.DomainReassign; p != nil {
			return appV20Identifiers(id("domain", p.Domain), id("new owner id", p.NewOwnerID), id("parent domain", p.ParentDomain), id("proposal id", p.ProposalID))
		}
	case tx.TxTypeCoCommitSubmit:
		if p := parsed.CoCommitSubmit; p != nil {
			if err := appV20Identifiers(id("shared id", p.SharedID), id("domain", p.Domain)); err != nil {
				return err
			}
			if err := appV20ExactBytes("co-commit content hash", p.ContentHash, sha256.Size, false); err != nil {
				return err
			}
			if len(p.AgreementNonce) > maxAppV20IdentifierBytes {
				return fmt.Errorf("agreement nonce has %d bytes, limit %d", len(p.AgreementNonce), maxAppV20IdentifierBytes)
			}
			for i, author := range p.Coauthors {
				if err := appV20Identifier(fmt.Sprintf("coauthor[%d] chain id", i), author.ChainID); err != nil {
					return err
				}
				if err := appV20ExactBytes(fmt.Sprintf("coauthor[%d] public key", i), author.PubKey, ed25519.PublicKeySize, false); err != nil {
					return err
				}
				if err := appV20ExactBytes(fmt.Sprintf("coauthor[%d] signature", i), author.Sig, ed25519.SignatureSize, false); err != nil {
					return err
				}
			}
		}
	case tx.TxTypeCoCommitAttest:
		if p := parsed.CoCommitAttest; p != nil {
			if err := appV20Identifiers(id("shared id", p.SharedID), id("peer chain id", p.PeerChainID)); err != nil {
				return err
			}
			if err := appV20ExactBytes("attestation peer public key", p.PeerPubKey, ed25519.PublicKeySize, false); err != nil {
				return err
			}
			if err := appV20ExactBytes("attestation peer signature", p.PeerSig, ed25519.SignatureSize, false); err != nil {
				return err
			}
			if err := appV20ExactBytes("attestation core hash", p.CoreHash, sha256.Size, false); err != nil {
				return err
			}
			if len(p.Receipt) > maxAppV20MetadataBytes {
				return fmt.Errorf("commit receipt has %d bytes, limit %d", len(p.Receipt), maxAppV20MetadataBytes)
			}
		}
	case tx.TxTypeCrossFedSet:
		if p := parsed.CrossFedTerms; p != nil {
			if err := appV20Identifier("remote chain id", p.RemoteChainID); err != nil {
				return err
			}
			if err := appV20ExactBytes("cross-federation peer public key", p.PeerPubKey, ed25519.PublicKeySize, false); err != nil {
				return err
			}
			if err := appV20Metadata("federation endpoint", p.Endpoint); err != nil {
				return err
			}
			if err := appV20StringCollection("allowed domains", p.AllowedDomains); err != nil {
				return err
			}
			if err := appV20StringCollection("allowed departments", p.AllowedDepts); err != nil {
				return err
			}
			return appV20EncodedRecord("cross-federation terms", 4+len(p.Endpoint)+len(p.PeerPubKey)+32+appV20StringsSize(p.AllowedDomains)+appV20StringsSize(p.AllowedDepts))
		}
	case tx.TxTypeCrossFedRevoke:
		if p := parsed.CrossFedRevoke; p != nil {
			if err := appV20Identifier("remote chain id", p.RemoteChainID); err != nil {
				return err
			}
			return appV20Metadata("cross-federation revoke reason", p.Reason)
		}
	}
	return nil
}
