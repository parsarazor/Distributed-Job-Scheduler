package service

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"
)

// PostgreSQL jsonb expands exponent notation when returning JSON. Bound that
// expansion before admission so a tiny numeric input cannot create a huge row
// or an individually unreadable GetJob response.
func payloadExpansionFits(payload []byte) bool {
	if len(payload) == 0 {
		return true
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	estimated := len(payload)
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return true
		}
		if err != nil {
			return false
		}
		number, ok := token.(json.Number)
		if !ok {
			continue
		}
		text := string(number)
		coefficient := text
		exponent := 0
		if at := strings.IndexAny(text, "eE"); at >= 0 {
			coefficient = text[:at]
			n, err := strconv.ParseInt(text[at+1:], 10, 32)
			if err != nil {
				return false
			}
			exponent = int(n)
		}
		sign := 0
		if strings.HasPrefix(coefficient, "-") {
			sign = 1
			coefficient = coefficient[1:]
		}
		point, digits := len(coefficient), len(coefficient)
		if at := strings.IndexByte(coefficient, '.'); at >= 0 {
			point = at
			digits--
		}
		point += exponent
		// Match PostgreSQL's maximum integer and fractional precision too.
		if point > 131072 || digits-point > 16383 {
			return false
		}
		expanded := digits + 1
		if point <= 0 {
			expanded = 2 - point + digits
		} else if point >= digits {
			expanded = point
		}
		estimated += max(0, expanded+sign-len(text))
		if estimated > 1<<20 {
			return false
		}
	}
}
