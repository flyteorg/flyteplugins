package ioutils

import (
	"context"
	"hash/fnv"
	"strings"

	"github.com/pkg/errors"
)

func GenerateAlphabet(b []rune) []rune {
	for i := 'a'; i <= 'z'; i++ {
		b = append(b, i)
	}
	return b
}

func GenerateArabicNumerals(b []rune) []rune {
	for i := '0'; i <= '9'; i++ {
		b = append(b, i)
	}
	return b
}

func createAlphabetAndNumerals() []rune {
	b := make([]rune, 0, 36)
	b = GenerateAlphabet(b)
	return GenerateArabicNumerals(b)
}

// this sharder distributes data into one of the precomputed buckets. The precomputed shards in this specific case
// are of the format {[0-9a-z][0-9a-z]} 2 character long. The bucket is deterministically determined given the input s
type BucketsDataSharder struct {
	precomputedPrefixes []string
	buckets             uint32
}

func (d *BucketsDataSharder) Initialize(ctx context.Context) error {
	permittedChars := createAlphabetAndNumerals()
	n := len(permittedChars)
	precomputedPrefixes := make([]string, 0, n*n)
	for _, c1 := range permittedChars {
		for _, c2 := range permittedChars {
			sb := strings.Builder{}
			sb.WriteRune(c1)
			sb.WriteRune(c2)
			precomputedPrefixes = append(precomputedPrefixes, sb.String())
		}
	}
	d.precomputedPrefixes = precomputedPrefixes
	d.buckets = uint32(n * n)
	return nil
}

// Generates deterministic shard id for the given string s
func (d *BucketsDataSharder) GetShardPrefix(ctx context.Context, s []byte) (string, error) {
	h := fnv.New32a()
	_, err := h.Write(s)
	if err != nil {
		return "", errors.Wrap(err, "failed to create shard prefix, reason hash failure.")
	}
	idx := h.Sum32() % d.buckets
	return d.precomputedPrefixes[idx], nil
}
