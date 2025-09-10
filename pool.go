package kahflane

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// PoolConfig extends ConnectionConfig with pool-specific settings
type PoolConfig struct {
	ConnectionConfig
	MaxConnections int           // Maximum number of connections in pool (default: 10)
	MinConnections int           // Minimum number of connections in pool (default: 2)
	AcquireTimeout time.Duration // Timeout for acquiring a connection (default: 30s)
	IdleTimeout    time.Duration // Time before idle connections are closed (default: 5m)
	MaxLifetime    time.Duration // Maximum lifetime of a connection (default: 1h)
}

// DefaultPoolConfig returns a pool config with sensible defaults
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		ConnectionConfig: DefaultConnectionConfig(),
		MaxConnections:   10,
		MinConnections:   2,
		AcquireTimeout:   30 * time.Second,
		IdleTimeout:      5 * time.Minute,
		MaxLifetime:      1 * time.Hour,
	}
}

// Pool manages a pool of database connections
type Pool struct {
	config      PoolConfig
	connections []*pooledConnection
	available   chan *pooledConnection
	mutex       sync.RWMutex
	closed      bool
	wg          sync.WaitGroup

	// Statistics
	stats struct {
		sync.RWMutex
		totalConnections     int
		activeConnections    int
		availableConnections int
		totalQueries         int64
		failedQueries        int64
	}
}

type pooledConnection struct {
	client    *Client
	createdAt time.Time
	lastUsed  time.Time
	inUse     bool
	pool      *Pool
}

// NewPool creates a new connection pool
func NewPool(config PoolConfig) (*Pool, error) {
	if config.MaxConnections <= 0 {
		config.MaxConnections = 10
	}
	if config.MinConnections < 0 {
		config.MinConnections = 0
	}
	if config.MinConnections > config.MaxConnections {
		config.MinConnections = config.MaxConnections
	}
	if config.AcquireTimeout == 0 {
		config.AcquireTimeout = 30 * time.Second
	}

	pool := &Pool{
		config:    config,
		available: make(chan *pooledConnection, config.MaxConnections),
	}

	// Initialize minimum connections
	if err := pool.initializeMinConnections(); err != nil {
		return nil, fmt.Errorf("failed to initialize pool: %w", err)
	}

	// Start background maintenance
	pool.wg.Add(1)
	go pool.maintainConnections()

	return pool, nil
}

// Acquire gets a connection from the pool
func (p *Pool) Acquire(ctx context.Context) (*Client, error) {
	if p.isClosed() {
		return nil, fmt.Errorf("pool is closed")
	}

	// Try to get an available connection
	select {
	case conn := <-p.available:
		if conn.client.IsConnected() {
			conn.inUse = true
			conn.lastUsed = time.Now()
			p.updateStats(func() {
				p.stats.activeConnections++
				p.stats.availableConnections--
			})
			return conn.client, nil
		}
		// Connection is dead, discard it
		p.removeConnection(conn)
	default:
		// No available connections
	}

	// Try to create a new connection
	if p.canCreateConnection() {
		conn, err := p.createConnection()
		if err == nil {
			conn.inUse = true
			conn.lastUsed = time.Now()
			p.updateStats(func() {
				p.stats.activeConnections++
			})
			return conn.client, nil
		}
	}

	// Wait for an available connection
	acquireCtx, cancel := context.WithTimeout(ctx, p.config.AcquireTimeout)
	defer cancel()

	select {
	case conn := <-p.available:
		if conn.client.IsConnected() {
			conn.inUse = true
			conn.lastUsed = time.Now()
			p.updateStats(func() {
				p.stats.activeConnections++
				p.stats.availableConnections--
			})
			return conn.client, nil
		}
		// Connection is dead, discard it and try again
		p.removeConnection(conn)
		return p.Acquire(ctx)
	case <-acquireCtx.Done():
		return nil, fmt.Errorf("timeout acquiring connection from pool")
	}
}

// Release returns a connection to the pool
func (p *Pool) Release(client *Client) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Find the connection
	for _, conn := range p.connections {
		if conn.client == client {
			if !conn.inUse {
				return // Already released
			}

			conn.inUse = false
			conn.lastUsed = time.Now()

			// Check if connection is still valid
			if client.IsConnected() && !p.shouldDiscardConnection(conn) {
				select {
				case p.available <- conn:
					p.updateStats(func() {
						p.stats.activeConnections--
						p.stats.availableConnections++
					})
				default:
					// Channel is full, discard this connection
					p.removeConnectionLocked(conn)
				}
			} else {
				p.removeConnectionLocked(conn)
			}
			return
		}
	}
}

// Query executes a query using a connection from the pool
func (p *Pool) Query(ctx context.Context, sql string, options ...*QueryOptions) (*SQLResult, error) {
	client, err := p.Acquire(ctx)
	if err != nil {
		p.updateStats(func() {
			p.stats.failedQueries++
		})
		return nil, err
	}
	defer p.Release(client)

	p.updateStats(func() {
		p.stats.totalQueries++
	})

	result, err := client.Query(sql, options...)
	if err != nil {
		p.updateStats(func() {
			p.stats.failedQueries++
		})
	}

	return result, err
}

// Execute is an alias for Query for convenience
func (p *Pool) Execute(ctx context.Context, sql string, parameters []interface{}, database ...string) (*SQLResult, error) {
	opts := &QueryOptions{
		Parameters: parameters,
	}
	if len(database) > 0 {
		opts.Database = database[0]
	}
	return p.Query(ctx, sql, opts)
}

// Stats returns pool statistics
func (p *Pool) Stats() map[string]interface{} {
	p.stats.RLock()
	defer p.stats.RUnlock()

	return map[string]interface{}{
		"total_connections":     p.stats.totalConnections,
		"active_connections":    p.stats.activeConnections,
		"available_connections": p.stats.availableConnections,
		"max_connections":       p.config.MaxConnections,
		"min_connections":       p.config.MinConnections,
		"total_queries":         p.stats.totalQueries,
		"failed_queries":        p.stats.failedQueries,
	}
}

// Close closes all connections in the pool
func (p *Pool) Close() error {
	p.mutex.Lock()
	if p.closed {
		p.mutex.Unlock()
		return nil
	}
	p.closed = true

	// Close all connections
	for _, conn := range p.connections {
		conn.client.Disconnect()
	}
	p.connections = nil

	// Close the available channel
	close(p.available)
	p.mutex.Unlock()

	// Wait for background goroutines to finish
	p.wg.Wait()

	return nil
}

// Private methods

func (p *Pool) initializeMinConnections() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for i := 0; i < p.config.MinConnections; i++ {
		conn, err := p.createConnectionLocked()
		if err != nil {
			// Log error but continue - we'll try to create connections later
			continue
		}

		select {
		case p.available <- conn:
			p.stats.availableConnections++
		default:
			// This shouldn't happen during initialization
			conn.client.Disconnect()
		}
	}

	return nil
}

func (p *Pool) createConnection() (*pooledConnection, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.createConnectionLocked()
}

func (p *Pool) createConnectionLocked() (*pooledConnection, error) {
	if len(p.connections) >= p.config.MaxConnections {
		return nil, fmt.Errorf("maximum connections reached")
	}

	client := NewClient(p.config.ConnectionConfig)

	// Set up event handlers for pool management
	client.SetEventHandlers(map[string]interface{}{
		"disconnected": func(reason string) {
			// Connection lost, remove from pool
			p.handleConnectionLost(client)
		},
	})

	_, err := client.Connect()
	if err != nil {
		return nil, err
	}

	conn := &pooledConnection{
		client:    client,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
		pool:      p,
	}

	p.connections = append(p.connections, conn)
	p.stats.totalConnections++

	return conn, nil
}

func (p *Pool) canCreateConnection() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return len(p.connections) < p.config.MaxConnections && !p.closed
}

func (p *Pool) removeConnection(conn *pooledConnection) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.removeConnectionLocked(conn)
}

func (p *Pool) removeConnectionLocked(conn *pooledConnection) {
	for i, c := range p.connections {
		if c == conn {
			c.client.Disconnect()
			p.connections = append(p.connections[:i], p.connections[i+1:]...)
			p.stats.totalConnections--
			if c.inUse {
				p.stats.activeConnections--
			}
			break
		}
	}
}

func (p *Pool) shouldDiscardConnection(conn *pooledConnection) bool {
	now := time.Now()

	// Check maximum lifetime
	if p.config.MaxLifetime > 0 && now.Sub(conn.createdAt) > p.config.MaxLifetime {
		return true
	}

	// Check idle timeout
	if p.config.IdleTimeout > 0 && now.Sub(conn.lastUsed) > p.config.IdleTimeout {
		return true
	}

	return false
}

func (p *Pool) handleConnectionLost(client *Client) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for i, conn := range p.connections {
		if conn.client == client {
			p.connections = append(p.connections[:i], p.connections[i+1:]...)
			p.stats.totalConnections--
			if conn.inUse {
				p.stats.activeConnections--
			}
			break
		}
	}
}

func (p *Pool) maintainConnections() {
	defer p.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		if p.isClosed() {
			return
		}

		select {
		case <-ticker.C:
			p.cleanupIdleConnections()
			p.ensureMinConnections()
		case <-time.After(time.Minute):
			// Periodic check for closure
		}
	}
}

func (p *Pool) cleanupIdleConnections() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return
	}

	var toRemove []*pooledConnection

	for _, conn := range p.connections {
		if !conn.inUse && p.shouldDiscardConnection(conn) {
			toRemove = append(toRemove, conn)
		}
	}

	for _, conn := range toRemove {
		// Don't remove if we'd go below minimum connections
		if len(p.connections)-len(toRemove) >= p.config.MinConnections {
			p.removeConnectionLocked(conn)
		}
	}
}

func (p *Pool) ensureMinConnections() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return
	}

	needed := p.config.MinConnections - len(p.connections)
	for i := 0; i < needed && len(p.connections) < p.config.MaxConnections; i++ {
		conn, err := p.createConnectionLocked()
		if err != nil {
			continue
		}

		select {
		case p.available <- conn:
			p.stats.availableConnections++
		default:
			// Channel is full
			p.removeConnectionLocked(conn)
			break
		}
	}
}

func (p *Pool) isClosed() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.closed
}

func (p *Pool) updateStats(fn func()) {
	p.stats.Lock()
	fn()
	p.stats.Unlock()
}
