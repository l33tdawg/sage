package store

import (
	"encoding/base64"
	"errors"
	"strings"
	"time"
)

const taskNoticeRawCursorPrefix = "task-v1:"

// DecodeTaskNoticeCursor accepts exact-key task cursors and previously issued
// inbox cursors. The timestamp spelling is retained; PostgreSQL interprets it
// as a timestamp, while SQLite must preserve the raw TEXT ordering key.
func DecodeTaskNoticeCursor(cursor string) (string, string, error) {
	createdAt, id, _, err := decodeTaskNoticeCursor(cursor)
	return createdAt, id, err
}

func decodeTaskNoticeCursor(cursor string) (string, string, bool, error) {
	if cursor == "" {
		return "", "", false, nil
	}
	if len(cursor) > 1024 {
		return "", "", false, errors.New("invalid task notice cursor")
	}
	legacy := !strings.HasPrefix(cursor, taskNoticeRawCursorPrefix)
	if !legacy {
		cursor = strings.TrimPrefix(cursor, taskNoticeRawCursorPrefix)
	} else if strings.HasPrefix(cursor, "task-") {
		return "", "", false, errors.New("invalid task notice cursor")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", false, errors.New("invalid task notice cursor")
	}
	createdAt, id, ok := strings.Cut(string(decoded), "|")
	if !ok || !validTaskNoticeCursorFields(createdAt, id) {
		return "", "", false, errors.New("invalid task notice cursor")
	}
	return createdAt, id, legacy, nil
}

func validTaskNoticeCursorFields(createdAt, id string) bool {
	_, err := time.Parse(time.RFC3339Nano, createdAt)
	return err == nil && id != "" && len(id) <= 256
}

func encodeTaskNoticeRawCursor(createdAt, id string) (string, error) {
	if !validTaskNoticeCursorFields(createdAt, id) {
		return "", errors.New("invalid task notice cursor")
	}
	cursor := taskNoticeRawCursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(createdAt+"|"+id))
	if len(cursor) > 1024 {
		return "", errors.New("invalid task notice cursor")
	}
	return cursor, nil
}

func decodeSQLiteTaskNoticeCursor(cursor string) (string, string, error) {
	createdAt, id, legacy, err := decodeTaskNoticeCursor(cursor)
	if err != nil || createdAt == "" || !legacy {
		return createdAt, id, err
	}
	// Both native notice writers omit created_at and use the schema's fixed
	// millisecond UTC default. Old time.Time cursors trimmed its trailing zeroes.
	// Restore only that legacy representation, without consulting a boundary
	// row that may since have been deleted. New versioned raw keys are untouched.
	timestamp, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return "", "", errors.New("invalid legacy SQLite task notice cursor")
	}
	if timestamp.Nanosecond()%int(time.Millisecond) != 0 {
		// Non-native legacy values were accepted by the old shared decoder.
		// Preserve that normalization and precision, without claiming recovery
		// of any non-native database timestamp's original spelling.
		return timestamp.UTC().Format(time.RFC3339Nano), id, nil
	}
	return timestamp.UTC().Format("2006-01-02T15:04:05.000Z"), id, nil
}
