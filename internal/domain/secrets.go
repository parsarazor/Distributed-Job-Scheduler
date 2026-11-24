package domain

import "strings"

// DemoWorkerToken is the development-only worker token shipped in Compose.
const DemoWorkerToken = "local-development-worker-token-change-me"

// ValidProductionToken requires at least 32 visible ASCII characters and
// rejects the demo token.
func ValidProductionToken(token string) bool {
	return len(token) >= 32 && token != DemoWorkerToken && !strings.ContainsFunc(token, func(r rune) bool {
		return r < '!' || r > '~'
	})
}

// DevelopmentPassword reports the well-known local passwords production rejects.
func DevelopmentPassword(password string) bool {
	return password == "scheduler" || password == "guest"
}
