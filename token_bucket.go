package tokenbucket

import (
	"time"
)

type TokenBucket struct {
	// capacity of tokens in the bucket
	maxToken int
	// the current number of tokens remaining in the bucket
	currentToken int
	//seconds it takes to fill the bucket
	rate int64
	//the time of the last
	lastTimestamp int64
}

func NewTokenBucket(rate int64, maximumTokens int) *TokenBucket {
	return &TokenBucket{
		maxToken:      maximumTokens,
		currentToken:  maximumTokens,
		rate:          rate,
		lastTimestamp: time.Now().Unix(),
	}
}

// checks if the request can be processed based on the current tokens in the bucket and the addition rate.
func (tb *TokenBucket) CheckRequest() bool {
	if time.Now().Unix()-tb.lastTimestamp > tb.rate {
		tb.lastTimestamp = time.Now().Unix()
		tb.currentToken = tb.maxToken
	}

	if tb.currentToken <= 0 {
		return false
	}

	tb.currentToken--
	return true
}
