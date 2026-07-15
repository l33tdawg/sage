package scope

import (
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	memorytags "github.com/l33tdawg/sage/internal/tags"
)

const maxContentBytes = 1 << 20

// Content is the canonical recoverable envelope for a scoped memory. It omits
// embeddings and other rebuildable projections, but retains every field needed
// to verify the normal memory hash and recreate the durable query projection.
type Content struct {
	MemoryID          string
	ScopeID           string
	ScopeRevision     uint64
	SubmittingAgentID string
	ContentHash       []byte
	MemoryType        byte
	Domain            string
	ConfidenceScore   float64
	Content           string
	ParentHash        string
	Classification    byte
	TaskStatus        string
	Tags              []string
	SubmittedHeight   int64
	SubmittedUnix     int64
}

func EncodeContent(content Content) ([]byte, error) {
	if err := ValidateContent(content); err != nil {
		return nil, err
	}
	buf := []byte{SchemaV1}
	buf = appendString(buf, content.MemoryID)
	buf = appendString(buf, content.ScopeID)
	buf = appendUint64(buf, content.ScopeRevision)
	buf = appendString(buf, content.SubmittingAgentID)
	buf = appendUint32(buf, uint32(len(content.ContentHash)))
	buf = append(buf, content.ContentHash...)
	buf = append(buf, content.MemoryType)
	buf = appendString(buf, content.Domain)
	buf = appendUint64(buf, math.Float64bits(content.ConfidenceScore))
	buf = appendString(buf, content.Content)
	buf = appendString(buf, content.ParentHash)
	buf = append(buf, content.Classification)
	buf = appendString(buf, content.TaskStatus)
	buf = appendInt64(buf, content.SubmittedHeight)
	buf = appendInt64(buf, content.SubmittedUnix)
	if len(content.Tags) > 0 {
		buf = append(buf, scopedContentTagsMarker...)
		buf = appendUint32(buf, uint32(len(content.Tags))) // #nosec G115 -- bounded by tags.MaxCount
		for _, tag := range content.Tags {
			buf = appendString(buf, tag)
		}
	}
	return buf, nil
}

var scopedContentTagsMarker = []byte("SAGE-CONTENT-TAGS-V1\x00")

func DecodeContent(data []byte) (Content, error) {
	if len(data) == 0 || data[0] != SchemaV1 {
		return Content{}, errors.New("unsupported or empty scoped content schema")
	}
	off := 1
	var content Content
	var err error
	if content.MemoryID, off, err = readString(data, off, maxIDBytes); err != nil {
		return Content{}, fmt.Errorf("memory id: %w", err)
	}
	if content.ScopeID, off, err = readString(data, off, maxIDBytes); err != nil {
		return Content{}, fmt.Errorf("scope id: %w", err)
	}
	if content.ScopeRevision, off, err = readUint64(data, off); err != nil {
		return Content{}, fmt.Errorf("scope revision: %w", err)
	}
	if content.SubmittingAgentID, off, err = readString(data, off, maxIDBytes); err != nil {
		return Content{}, fmt.Errorf("submitting agent: %w", err)
	}
	hashLen, next, err := readUint32(data, off)
	if err != nil || hashLen != 32 || int(hashLen) > len(data)-next {
		return Content{}, errors.New("content hash must be 32 bytes")
	}
	off = next
	content.ContentHash = append([]byte(nil), data[off:off+int(hashLen)]...)
	off += int(hashLen)
	if off >= len(data) {
		return Content{}, errors.New("memory type: truncated")
	}
	content.MemoryType = data[off]
	off++
	if content.Domain, off, err = readString(data, off, maxDomainBytes); err != nil {
		return Content{}, fmt.Errorf("domain: %w", err)
	}
	confidenceBits, next, err := readUint64(data, off)
	if err != nil {
		return Content{}, fmt.Errorf("confidence: %w", err)
	}
	content.ConfidenceScore = math.Float64frombits(confidenceBits)
	off = next
	if content.Content, off, err = readString(data, off, maxContentBytes); err != nil {
		return Content{}, fmt.Errorf("content: %w", err)
	}
	if content.ParentHash, off, err = readString(data, off, maxIDBytes); err != nil {
		return Content{}, fmt.Errorf("parent hash: %w", err)
	}
	if off >= len(data) {
		return Content{}, errors.New("classification: truncated")
	}
	content.Classification = data[off]
	off++
	if content.TaskStatus, off, err = readString(data, off, maxIDBytes); err != nil {
		return Content{}, fmt.Errorf("task status: %w", err)
	}
	if content.SubmittedHeight, off, err = readInt64(data, off); err != nil {
		return Content{}, fmt.Errorf("submitted height: %w", err)
	}
	if content.SubmittedUnix, off, err = readInt64(data, off); err != nil {
		return Content{}, fmt.Errorf("submitted unix time: %w", err)
	}
	if off < len(data) {
		if len(data)-off < len(scopedContentTagsMarker) || string(data[off:off+len(scopedContentTagsMarker)]) != string(scopedContentTagsMarker) {
			return Content{}, errors.New("scoped content has invalid trailing bytes")
		}
		off += len(scopedContentTagsMarker)
		count, next, countErr := readUint32(data, off)
		if countErr != nil || count == 0 || count > memorytags.MaxCount {
			return Content{}, errors.New("scoped content has invalid tag count")
		}
		off = next
		content.Tags = make([]string, 0, int(count))
		for i := uint32(0); i < count; i++ {
			tag, tagNext, tagErr := readString(data, off, memorytags.MaxBytes)
			if tagErr != nil {
				return Content{}, fmt.Errorf("tag %d: %w", i, tagErr)
			}
			content.Tags = append(content.Tags, tag)
			off = tagNext
		}
	}
	if off != len(data) {
		return Content{}, errors.New("scoped content has trailing bytes")
	}
	if err := ValidateContent(content); err != nil {
		return Content{}, err
	}
	return content, nil
}

func ValidateContent(content Content) error {
	if err := validateID("memory id", content.MemoryID); err != nil {
		return err
	}
	if err := validateID("scope id", content.ScopeID); err != nil {
		return err
	}
	if content.ScopeRevision == 0 || content.SubmittedHeight <= 0 || content.SubmittedUnix <= 0 {
		return errors.New("scope revision, submitted height, and submitted unix time must be positive")
	}
	if err := validateID("submitting agent", content.SubmittingAgentID); err != nil {
		return err
	}
	if len(content.ContentHash) != 32 {
		return errors.New("content hash must be 32 bytes")
	}
	if content.MemoryType < 1 || content.MemoryType > 4 {
		return fmt.Errorf("invalid memory type %d", content.MemoryType)
	}
	if err := validateString("domain", content.Domain, maxDomainBytes); err != nil {
		return err
	}
	if math.IsNaN(content.ConfidenceScore) || math.IsInf(content.ConfidenceScore, 0) || content.ConfidenceScore < 0 || content.ConfidenceScore > 1 {
		return errors.New("confidence score must be finite and within 0..1")
	}
	if err := validateString("content", content.Content, maxContentBytes); err != nil {
		return err
	}
	if len(content.ParentHash) > maxIDBytes || len(content.TaskStatus) > maxIDBytes {
		return errors.New("parent hash or task status is oversized")
	}
	if (content.ParentHash != "" && !utf8.ValidString(content.ParentHash)) || (content.TaskStatus != "" && !utf8.ValidString(content.TaskStatus)) {
		return errors.New("parent hash or task status is not valid UTF-8")
	}
	if content.Classification > 4 {
		return fmt.Errorf("invalid classification %d", content.Classification)
	}
	if err := memorytags.ValidateCanonical(content.Tags); err != nil {
		return fmt.Errorf("tags: %w", err)
	}
	return nil
}
