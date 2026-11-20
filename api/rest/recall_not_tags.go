package rest

import (
	"errors"
	"fmt"

	memorytags "github.com/l33tdawg/sage/internal/tags"
)

// recallNotTags validates the not_tags recall filter and returns it in
// canonical form (sorted, de-duplicated, same limits as stored tags).
//
// It is local-only. Federated recall carries a signed query whose filter set
// peers verify field by field, and older peers would silently drop an unknown
// exclusion and return exactly the records the caller asked to exclude. So a
// federated request carrying not_tags is refused instead of being widened.
func recallNotTags(values []string, federated bool, federateChains []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if federated || len(federateChains) > 0 {
		return nil, errors.New("not_tags is supported on local recall only; drop it or set federated=false")
	}
	canonical, err := memorytags.Normalize(values)
	if err != nil {
		return nil, fmt.Errorf("not_tags: %w", err)
	}
	return canonical, nil
}
