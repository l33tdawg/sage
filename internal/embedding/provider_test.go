package embedding

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashProvider_Embed(t *testing.T) {
	p := NewHashProvider(768)
	emb, err := p.Embed(context.Background(), "test text")
	require.NoError(t, err)
	assert.Len(t, emb, 768)

	// Verify unit vector (norm ~= 1.0)
	var norm float64
	for _, v := range emb {
		norm += float64(v) * float64(v)
	}
	norm = math.Sqrt(norm)
	assert.InDelta(t, 1.0, norm, 0.001)
}

func TestHashProvider_Dimension(t *testing.T) {
	p := NewHashProvider(512)
	assert.Equal(t, 512, p.Dimension())

	// Default dimension
	p2 := NewHashProvider(0)
	assert.Equal(t, 768, p2.Dimension())
}

func TestHashProvider_Ready(t *testing.T) {
	p := NewHashProvider(768)
	assert.True(t, p.Ready())
}

func TestHashProvider_Consistency(t *testing.T) {
	p := NewHashProvider(768)
	ctx := context.Background()

	emb1, err := p.Embed(ctx, "same text")
	require.NoError(t, err)
	emb2, err := p.Embed(ctx, "same text")
	require.NoError(t, err)

	assert.Equal(t, emb1, emb2)
}

func TestHashProvider_DifferentTexts(t *testing.T) {
	p := NewHashProvider(768)
	ctx := context.Background()

	emb1, err := p.Embed(ctx, "text one")
	require.NoError(t, err)
	emb2, err := p.Embed(ctx, "text two")
	require.NoError(t, err)

	// They should differ
	differ := false
	for i := range emb1 {
		if emb1[i] != emb2[i] {
			differ = true
			break
		}
	}
	assert.True(t, differ, "different texts should produce different embeddings")
}

func TestOllamaClient_ImplementsProvider(t *testing.T) {
	var _ Provider = (*Client)(nil)
}


func TestOllamaClient_Dimension(t *testing.T) {
	c := NewClient("", "")
	assert.Equal(t, 768, c.Dimension())
}

func TestOllamaClient_Ready(t *testing.T) {
	c := NewClient("", "")
	assert.False(t, c.Ready())
}
