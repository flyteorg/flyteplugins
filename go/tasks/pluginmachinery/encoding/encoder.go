package encoding

import (
	"encoding/base32"
	"fmt"
	"hash/fnv"
	"strings"
)

const specialEncoderKey = "abcdefghijklmnopqrstuvwxyz123456"

var Base32Encoder = base32.NewEncoding(specialEncoderKey).WithPadding(base32.NoPadding)

// FixedLengthUniqueID creates a new UniqueID that is based on the inputID and of a specified length, if the given id is
// longer than the maxLength.
func FixedLengthUniqueID(inputID string, maxLength int) (string, error) {
	if len(inputID) <= maxLength {
		return inputID, nil
	}

	hasher := fnv.New64a()
	// Using 64a an error can never happen, so this will always remain not covered by a unit test
	_, _ = hasher.Write([]byte(inputID)) // #nosec
	// The maximum size of b is 64/8(bits) = 8 bytes
	b := hasher.Sum(nil)

	// Encoding Length Calculation:
	// Base32 Encoder will encode every 5 bits into an output character (2 ^ 5 = 32)
	// output length = ciel(<input bits> / 5) = ceil(64 / 5) = 13
	// We prefix with character `f` so the final output is 14

	// expected length after this step is 14 chars (1 + 13 chars from Base32Encoder.EncodeToString(b))
	finalStr := "f" + Base32Encoder.EncodeToString(b)
	if len(finalStr) > maxLength {
		return finalStr, fmt.Errorf("max Length is too small, cannot create an encoded string that is so small")
	}
	return finalStr, nil
}

// FixedLengthUniqueIDForParts creates a new uniqueID using the parts concatenated using `-` and ensures that the
// uniqueID is not longer than the maxLength. In case a simple concatenation yields a longer string, a new hashed ID is
// created which is always around 8 characters in length.
func FixedLengthUniqueIDForParts(maxLength int, parts ...string) (string, error) {
	b := strings.Builder{}
	for i, p := range parts {
		if i > 0 && b.Len() > 0 {
			// Ignoring the error as it always returns a nil error
			_, _ = b.WriteRune('-') // #nosec
		}

		// Ignoring the error as this is always nil
		_, _ = b.WriteString(p) // #nosec
	}

	return FixedLengthUniqueID(b.String(), maxLength)
}
