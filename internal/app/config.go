package app

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

const demoWorkerToken = "local-development-worker-token-change-me"

type serverConfig struct {
	development bool
	workerToken string
	adminToken  string
	tls         *tls.Config
	seedDemo    bool
}

func loadServerConfig(storage string) (serverConfig, error) {
	cfg := serverConfig{workerToken: os.Getenv("WORKER_TOKEN"), adminToken: os.Getenv("ADMIN_TOKEN")}
	switch os.Getenv("APP_ENV") {
	case "development":
		cfg.development = true
	case "", "production":
	default:
		return cfg, fmt.Errorf("APP_ENV must be development or production")
	}
	if storage == "memory" && !cfg.development {
		return cfg, fmt.Errorf("STORAGE=memory requires APP_ENV=development")
	}
	switch os.Getenv("DEMO_SEED") {
	case "", "false":
	case "true":
		if storage != "memory" || !cfg.development {
			return cfg, fmt.Errorf("DEMO_SEED=true requires development memory storage")
		}
		cfg.seedDemo = true
	default:
		return cfg, fmt.Errorf("DEMO_SEED must be true or false")
	}
	cert, key := os.Getenv("TLS_CERT_FILE"), os.Getenv("TLS_KEY_FILE")
	switch {
	case cert == "" && key == "" && cfg.development:
	case cert == "" || key == "":
		return cfg, fmt.Errorf("both TLS_CERT_FILE and TLS_KEY_FILE are required for gRPC TLS")
	default:
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return cfg, fmt.Errorf("load gRPC TLS: %w", err)
		}
		leaf, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			return cfg, fmt.Errorf("parse gRPC certificate: %w", err)
		}
		now := time.Now()
		if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
			return cfg, fmt.Errorf("gRPC certificate is outside its validity period")
		}
		if len(leaf.ExtKeyUsage)+len(leaf.UnknownExtKeyUsage) > 0 {
			serverUsage := false
			for _, usage := range leaf.ExtKeyUsage {
				serverUsage = serverUsage || usage == x509.ExtKeyUsageAny || usage == x509.ExtKeyUsageServerAuth
			}
			if !serverUsage {
				return cfg, fmt.Errorf("gRPC certificate does not allow server authentication")
			}
		}
		cfg.tls = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	}
	if cfg.adminToken != "" && cfg.adminToken == cfg.workerToken {
		return cfg, fmt.Errorf("ADMIN_TOKEN and WORKER_TOKEN must be distinct")
	}
	if !cfg.development {
		if !validProductionToken(cfg.workerToken) {
			return cfg, fmt.Errorf("WORKER_TOKEN must contain at least 32 visible ASCII characters and must not use the demo token")
		}
		if cfg.adminToken != "" && !validProductionToken(cfg.adminToken) {
			return cfg, fmt.Errorf("ADMIN_TOKEN must contain at least 32 visible ASCII characters and must not use the demo token")
		}
	}
	return cfg, nil
}

func validProductionToken(token string) bool {
	return len(token) >= 32 && token != demoWorkerToken && !strings.ContainsFunc(token, func(r rune) bool {
		return r < '!' || r > '~'
	})
}

func parseDatabaseConfig(raw string, development bool) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		// pgx parse errors can contain the connection string.
		return nil, errors.New("invalid DATABASE_URL")
	}
	if development {
		return cfg, nil
	}
	conn := cfg.ConnConfig
	if developmentPassword(conn.Password) {
		return nil, fmt.Errorf("DATABASE_URL uses a development password")
	}
	verifiedTLS := func(host string, config *tls.Config) bool {
		return host != "" && !strings.HasPrefix(host, "/") && config != nil &&
			!config.InsecureSkipVerify && config.ServerName != ""
	}
	if !verifiedTLS(conn.Host, conn.TLSConfig) {
		return nil, fmt.Errorf("DATABASE_URL requires verified TCP TLS (sslmode=verify-full)")
	}
	conn.TLSConfig.MinVersion = tls.VersionTLS12
	for _, fallback := range conn.Fallbacks {
		if !verifiedTLS(fallback.Host, fallback.TLSConfig) {
			return nil, fmt.Errorf("DATABASE_URL contains a fallback without verified TCP TLS")
		}
		fallback.TLSConfig.MinVersion = tls.VersionTLS12
	}
	return cfg, nil
}

func validateRabbitURL(raw string, development bool) error {
	uri, err := amqp.ParseURI(raw)
	if err != nil {
		return fmt.Errorf("invalid RABBITMQ_URL")
	}
	if development {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || uri.Scheme != "amqps" || u.Hostname() == "" {
		return fmt.Errorf("RABBITMQ_URL requires amqps with an explicit hostname")
	}
	if u.User == nil || u.User.Username() == "" {
		return fmt.Errorf("RABBITMQ_URL requires an explicit production credential")
	}
	password, _ := u.User.Password()
	if password == "" || developmentPassword(password) {
		return fmt.Errorf("RABBITMQ_URL requires a non-development password")
	}
	if (uri.CertFile == "") != (uri.KeyFile == "") {
		return fmt.Errorf("RABBITMQ_URL requires both client certificate and key when configuring mutual TLS")
	}
	return nil
}

func developmentPassword(password string) bool {
	return password == "scheduler" || password == "guest"
}

// ListenRPC binds the gRPC listener. Binding to a wider interface is explicit.
func ListenRPC(defaultPort string) (net.Listener, error) {
	host, port := os.Getenv("GRPC_HOST"), os.Getenv("PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = defaultPort
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("PORT must be a TCP port number")
	}
	return listenTCP(net.JoinHostPort(host, strconv.FormatUint(portNumber, 10)))
}

func listenTCP(address string) (net.Listener, error) {
	if os.Getenv("STORAGE") == "memory" {
		host, _, err := net.SplitHostPort(address)
		if err != nil || !net.ParseIP(host).IsLoopback() {
			return nil, errors.New("development memory storage requires a literal loopback listener")
		}
	}
	return net.Listen("tcp", address)
}
