package kahflane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Event types for client callbacks
type (
	ConnectedHandler       func(connectionInfo ConnectionInfo)
	DisconnectedHandler    func(reason string)
	ErrorHandler           func(error string, messageID *string)
	MessageHandler         func(message *Message)
	SQLResultHandler       func(result *SQLResult, executionID, sql, database string)
	PongHandler            func(messageID, timestamp string)
	BackupProgressHandler  func(progress *BackupProgress)
	RestoreProgressHandler func(progress *RestoreProgress)
	MaintenanceHandler     func(status *MaintenanceStatus)
)

// Client represents a connection to the KahfLane database server
type Client struct {
	config         ConnectionConfig
	conn           *websocket.Conn
	connectionInfo *ConnectionInfo

	// State
	connected      int32 // atomic
	connecting     int32 // atomic
	messageCounter int64 // atomic

	// Channels
	writeChan     chan []byte
	closeChan     chan struct{}
	reconnectChan chan struct{}

	// Synchronization
	mutex          sync.RWMutex
	pendingQueries map[string]*pendingQuery

	// Event handlers
	onConnected       ConnectedHandler
	onDisconnected    DisconnectedHandler
	onError           ErrorHandler
	onMessage         MessageHandler
	onSQLResult       SQLResultHandler
	onPong            PongHandler
	onBackupProgress  BackupProgressHandler
	onRestoreProgress RestoreProgressHandler
	onMaintenance     MaintenanceHandler

	// Reconnection
	reconnectAttempts int
	reconnectTimer    *time.Timer

	// JWT Authentication
	tokenInfo    *TokenInfo
	refreshTimer *time.Timer
	isRefreshing int32 // atomic

	// Context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

type pendingQuery struct {
	resultChan chan *SQLResult
	errorChan  chan error
	timeout    *time.Timer
}

// NewClient creates a new KahfLane database client
func NewClient(config ConnectionConfig) *Client {
	if config.Timeout == 0 {
		defaults := DefaultConnectionConfig()
		if config.URL != "" {
			defaults.URL = config.URL
		}
		if config.AccessToken != "" {
			defaults.AccessToken = config.AccessToken
		}
		if config.RefreshToken != "" {
			defaults.RefreshToken = config.RefreshToken
		}
		if config.AutoRefresh {
			defaults.AutoRefresh = config.AutoRefresh
		}
		if config.RefreshThreshold > 0 {
			defaults.RefreshThreshold = config.RefreshThreshold
		}
		config = defaults
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &Client{
		config:         config,
		writeChan:      make(chan []byte, 256),
		closeChan:      make(chan struct{}),
		reconnectChan:  make(chan struct{}),
		pendingQueries: make(map[string]*pendingQuery),
		ctx:            ctx,
		cancel:         cancel,
	}

	// Initialize JWT token info if provided
	if config.AccessToken != "" && config.RefreshToken != "" {
		client.tokenInfo = &TokenInfo{
			AccessToken:  config.AccessToken,
			RefreshToken: config.RefreshToken,
			ExpiresAt:    time.Now().Add(time.Hour), // Default 1 hour expiry
			TokenType:    "Bearer",
		}
	}

	return client
}

// SetEventHandlers sets event callback handlers
func (c *Client) SetEventHandlers(handlers map[string]interface{}) {
	if handler, ok := handlers["connected"].(ConnectedHandler); ok {
		c.onConnected = handler
	}
	if handler, ok := handlers["disconnected"].(DisconnectedHandler); ok {
		c.onDisconnected = handler
	}
	if handler, ok := handlers["error"].(ErrorHandler); ok {
		c.onError = handler
	}
	if handler, ok := handlers["message"].(MessageHandler); ok {
		c.onMessage = handler
	}
	if handler, ok := handlers["sql_result"].(SQLResultHandler); ok {
		c.onSQLResult = handler
	}
	if handler, ok := handlers["pong"].(PongHandler); ok {
		c.onPong = handler
	}
	if handler, ok := handlers["backup_progress"].(BackupProgressHandler); ok {
		c.onBackupProgress = handler
	}
	if handler, ok := handlers["restore_progress"].(RestoreProgressHandler); ok {
		c.onRestoreProgress = handler
	}
	if handler, ok := handlers["maintenance"].(MaintenanceHandler); ok {
		c.onMaintenance = handler
	}
}

// Connect establishes a connection to the database server
func (c *Client) Connect() (*ConnectionInfo, error) {
	if atomic.LoadInt32(&c.connected) == 1 {
		return nil, fmt.Errorf("already connected")
	}

	if !atomic.CompareAndSwapInt32(&c.connecting, 0, 1) {
		return nil, fmt.Errorf("connection already in progress")
	}
	defer atomic.StoreInt32(&c.connecting, 0)

	// Build connection URL with query parameters
	connectionURL, err := c.buildConnectionURL()
	if err != nil {
		return nil, fmt.Errorf("failed to build connection URL: %w", err)
	}

	// Create WebSocket connection
	dialer := websocket.Dialer{
		HandshakeTimeout: c.config.Timeout,
	}

	// Require JWT token for connection
	if c.tokenInfo == nil || c.tokenInfo.AccessToken == "" {
		return nil, fmt.Errorf("JWT token required for connection. Please login first")
	}

	// Prepare headers for JWT authentication
	headers := http.Header{}
	headers.Set("Authorization", c.tokenInfo.TokenType+" "+c.tokenInfo.AccessToken)

	conn, _, err := dialer.Dial(connectionURL, headers)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to WebSocket: %w", err)
	}

	c.conn = conn

	// Wait for authentication result
	authResult, err := c.waitForAuth()
	if err != nil {
		conn.Close()
		c.conn = nil
		return nil, err
	}

	// Connection successful
	atomic.StoreInt32(&c.connected, 1)
	c.reconnectAttempts = 0
	c.connectionInfo = authResult

	// Start message handling goroutines
	go c.readLoop()
	go c.writeLoop()
	go c.pingLoop()

	// Emit connected event
	if c.onConnected != nil {
		c.onConnected(*authResult)
	}

	return authResult, nil
}

// Query executes a SQL query and returns the result
func (c *Client) Query(sql string, options ...*QueryOptions) (*SQLResult, error) {
	if atomic.LoadInt32(&c.connected) == 0 {
		return nil, fmt.Errorf("not connected to database")
	}

	var opts *QueryOptions
	if len(options) > 0 && options[0] != nil {
		opts = options[0]
	} else {
		opts = &QueryOptions{}
	}

	messageID := c.generateMessageID()

	// Create message
	message := NewMessage("sql")
	message.MessageID = messageID
	message.SetData("sql", sql)
	if opts.Parameters != nil {
		message.SetData("parameters", opts.Parameters)
	}
	if opts.Database != "" {
		message.SetData("database", opts.Database)
	}

	// Setup pending query
	pending := &pendingQuery{
		resultChan: make(chan *SQLResult, 1),
		errorChan:  make(chan error, 1),
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = c.config.Timeout
	}

	pending.timeout = time.AfterFunc(timeout, func() {
		c.mutex.Lock()
		delete(c.pendingQueries, messageID)
		c.mutex.Unlock()

		select {
		case pending.errorChan <- fmt.Errorf("query timeout"):
		default:
		}
	})

	c.mutex.Lock()
	c.pendingQueries[messageID] = pending
	c.mutex.Unlock()

	// Send message
	messageBytes, err := message.ToJSON()
	if err != nil {
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	select {
	case c.writeChan <- messageBytes:
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	case <-time.After(c.config.WriteTimeout):
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("write timeout")
	}

	// Wait for result
	select {
	case result := <-pending.resultChan:
		c.cleanupPendingQuery(messageID)
		return result, nil
	case err := <-pending.errorChan:
		c.cleanupPendingQuery(messageID)
		return nil, err
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	}
}

// Execute is an alias for Query for convenience
func (c *Client) Execute(sql string, parameters []interface{}, database ...string) (*SQLResult, error) {
	opts := &QueryOptions{
		Parameters: parameters,
	}
	if len(database) > 0 {
		opts.Database = database[0]
	}
	return c.Query(sql, opts)
}

// CreateUser creates a new database user
// Requires SUPER privilege
func (c *Client) CreateUser(req *CreateUserRequest) (*UserManagementResult, error) {
	if atomic.LoadInt32(&c.connected) == 0 {
		return nil, fmt.Errorf("not connected to database")
	}

	messageID := c.generateMessageID()

	// Create message
	message := NewMessage("user_create")
	message.MessageID = messageID
	message.SetData("user", map[string]interface{}{
		"username": req.Username,
		"password": req.Password,
	})

	return c.executeUserManagementOperation(message, messageID, "user creation")
}

// DropUser drops a database user
// Requires SUPER privilege
func (c *Client) DropUser(req *DropUserRequest) (*UserManagementResult, error) {
	if atomic.LoadInt32(&c.connected) == 0 {
		return nil, fmt.Errorf("not connected to database")
	}

	messageID := c.generateMessageID()

	// Create message
	message := NewMessage("user_drop")
	message.MessageID = messageID
	message.SetData("user", map[string]interface{}{
		"username": req.Username,
	})

	return c.executeUserManagementOperation(message, messageID, "user drop")
}

// GrantPrivilege grants a privilege to a user
// Requires SUPER privilege
func (c *Client) GrantPrivilege(req *GrantPrivilegeRequest) (*UserManagementResult, error) {
	if atomic.LoadInt32(&c.connected) == 0 {
		return nil, fmt.Errorf("not connected to database")
	}

	messageID := c.generateMessageID()

	// Create message
	message := NewMessage("user_grant")
	message.MessageID = messageID
	message.SetData("grant", map[string]interface{}{
		"username":  req.Username,
		"host":      req.Host,
		"database":  req.Database,
		"table":     req.Table,
		"privilege": req.Privilege,
	})

	return c.executeUserManagementOperation(message, messageID, "grant privilege")
}

// RevokePrivilege revokes a privilege from a user
// Requires SUPER privilege
func (c *Client) RevokePrivilege(req *RevokePrivilegeRequest) (*UserManagementResult, error) {
	if atomic.LoadInt32(&c.connected) == 0 {
		return nil, fmt.Errorf("not connected to database")
	}

	messageID := c.generateMessageID()

	// Create message
	message := NewMessage("user_revoke")
	message.MessageID = messageID
	message.SetData("revoke", map[string]interface{}{
		"username":  req.Username,
		"database":  req.Database,
		"table":     req.Table,
		"privilege": req.Privilege,
	})

	return c.executeUserManagementOperation(message, messageID, "revoke privilege")
}

// ListUsers lists all database users
// Requires SUPER privilege
func (c *Client) ListUsers() (*UserListResult, error) {
	if atomic.LoadInt32(&c.connected) == 0 {
		return nil, fmt.Errorf("not connected to database")
	}

	messageID := c.generateMessageID()

	// Create message
	message := NewMessage("user_list")
	message.MessageID = messageID

	// Setup pending query
	pending := &pendingQuery{
		resultChan: make(chan *SQLResult, 1),
		errorChan:  make(chan error, 1),
	}

	timeout := c.config.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	pending.timeout = time.AfterFunc(timeout, func() {
		c.cleanupPendingQuery(messageID)
		select {
		case pending.errorChan <- fmt.Errorf("list users timeout"):
		default:
		}
	})

	c.mutex.Lock()
	c.pendingQueries[messageID] = pending
	c.mutex.Unlock()

	// Send message
	messageBytes, err := message.ToJSON()
	if err != nil {
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	select {
	case c.writeChan <- messageBytes:
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	case <-time.After(c.config.WriteTimeout):
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("write timeout")
	}

	// Wait for result
	select {
	case result := <-pending.resultChan:
		c.cleanupPendingQuery(messageID)
		// Parse users from the result data
		if result.Success {
			// Extract users from the Data field - assuming server sends it in the right format
			var userListResult UserListResult
			if result.Data != nil && len(result.Data) > 0 {
				// Convert the data to UserListResult format
				if usersData, ok := result.Data[0]["users"]; ok {
					userBytes, _ := json.Marshal(usersData)
					json.Unmarshal(userBytes, &userListResult.Users)
				}
				if countData, ok := result.Data[0]["count"]; ok {
					if count, ok := countData.(float64); ok {
						userListResult.Count = int(count)
					}
				}
			}
			userListResult.Success = true
			return &userListResult, nil
		}
		return &UserListResult{
			Success: false,
			Error:   result.Error,
		}, nil
	case err := <-pending.errorChan:
		c.cleanupPendingQuery(messageID)
		return nil, err
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	}
}

// ListDatabases retrieves a list of all databases
// CreateDatabase creates a new database
func (c *Client) CreateDatabase(req *CreateDatabaseRequest) (*DatabaseManagementResult, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("client is not connected")
	}

	messageID := c.generateMessageID()
	message := &Message{
		Type:      "database_create",
		MessageID: messageID,
	}
	message.SetData("database_name", req.DatabaseName)
	if req.Description != "" {
		message.SetData("description", req.Description)
	}

	errorChan := make(chan error, 1)
	timeout := time.NewTimer(30 * time.Second)

	c.mutex.Lock()
	c.pendingQueries[messageID] = &pendingQuery{
		resultChan: make(chan *SQLResult, 1),
		errorChan:  errorChan,
		timeout:    timeout,
	}
	c.mutex.Unlock()

	defer func() {
		timeout.Stop()
		c.cleanupPendingQuery(messageID)
	}()

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	select {
	case c.writeChan <- messageBytes:
	case <-c.ctx.Done():
		return nil, fmt.Errorf("client is shutting down")
	case <-timeout.C:
		return nil, fmt.Errorf("timeout sending message")
	}

	select {
	case err := <-errorChan:
		return nil, err
	case result := <-c.pendingQueries[messageID].resultChan:
		if result.Success && len(result.Data) > 0 {
			if resultData, ok := result.Data[0]["result"]; ok {
				if dbResult, ok := resultData.(*DatabaseManagementResult); ok {
					return dbResult, nil
				}
			}
		}
		return &DatabaseManagementResult{Success: false, Error: "invalid response format"}, nil
	case <-timeout.C:
		return nil, fmt.Errorf("timeout waiting for database create result")
	case <-c.ctx.Done():
		return nil, fmt.Errorf("client is shutting down")
	}
}

// DropDatabase drops an existing database
func (c *Client) DropDatabase(req *DropDatabaseRequest) (*DatabaseManagementResult, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("client is not connected")
	}

	messageID := c.generateMessageID()
	message := &Message{
		Type:      "database_drop",
		MessageID: messageID,
	}
	message.SetData("database_name", req.DatabaseName)
	if req.Force {
		message.SetData("force", req.Force)
	}

	errorChan := make(chan error, 1)
	timeout := time.NewTimer(30 * time.Second)

	c.mutex.Lock()
	c.pendingQueries[messageID] = &pendingQuery{
		resultChan: make(chan *SQLResult, 1),
		errorChan:  errorChan,
		timeout:    timeout,
	}
	c.mutex.Unlock()

	defer func() {
		timeout.Stop()
		c.cleanupPendingQuery(messageID)
	}()

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	select {
	case c.writeChan <- messageBytes:
	case <-c.ctx.Done():
		return nil, fmt.Errorf("client is shutting down")
	case <-timeout.C:
		return nil, fmt.Errorf("timeout sending message")
	}

	select {
	case err := <-errorChan:
		return nil, err
	case result := <-c.pendingQueries[messageID].resultChan:
		if result.Success && len(result.Data) > 0 {
			if resultData, ok := result.Data[0]["result"]; ok {
				if dbResult, ok := resultData.(*DatabaseManagementResult); ok {
					return dbResult, nil
				}
			}
		}
		return &DatabaseManagementResult{Success: false, Error: "invalid response format"}, nil
	case <-timeout.C:
		return nil, fmt.Errorf("timeout waiting for database drop result")
	case <-c.ctx.Done():
		return nil, fmt.Errorf("client is shutting down")
	}
}

// GetDatabaseInfo retrieves information about a specific database
func (c *Client) GetDatabaseInfo(databaseName string) (*DatabaseInfoResult, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("client is not connected")
	}

	messageID := c.generateMessageID()
	message := &Message{
		Type:      "database_info",
		MessageID: messageID,
		Data: map[string]interface{}{
			"database_name": databaseName,
		},
	}

	errorChan := make(chan error, 1)
	timeout := time.NewTimer(30 * time.Second)

	c.mutex.Lock()
	c.pendingQueries[messageID] = &pendingQuery{
		resultChan: make(chan *SQLResult, 1),
		errorChan:  errorChan,
		timeout:    timeout,
	}
	c.mutex.Unlock()

	defer func() {
		timeout.Stop()
		c.cleanupPendingQuery(messageID)
	}()

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	select {
	case c.writeChan <- messageBytes:
	case <-c.ctx.Done():
		return nil, fmt.Errorf("client is shutting down")
	case <-timeout.C:
		return nil, fmt.Errorf("timeout sending message")
	}

	select {
	case err := <-errorChan:
		return nil, err
	case result := <-c.pendingQueries[messageID].resultChan:
		if result.Success && len(result.Data) > 0 {
			if resultData, ok := result.Data[0]["result"]; ok {
				if dbResult, ok := resultData.(*DatabaseInfoResult); ok {
					return dbResult, nil
				}
			}
		}
		return &DatabaseInfoResult{Success: false, Error: "invalid response format"}, nil
	case <-timeout.C:
		return nil, fmt.Errorf("timeout waiting for database info result")
	case <-c.ctx.Done():
		return nil, fmt.Errorf("client is shutting down")
	}
}

func (c *Client) ListDatabases() ([]DatabaseInfo, error) {
	if atomic.LoadInt32(&c.connected) == 0 {
		return nil, fmt.Errorf("not connected to database")
	}

	messageID := c.generateMessageID()

	// Create message
	message := NewMessage("database_list")
	message.MessageID = messageID

	// Setup pending query
	pending := &pendingQuery{
		resultChan: make(chan *SQLResult, 1),
		errorChan:  make(chan error, 1),
	}

	timeout := c.config.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	pending.timeout = time.AfterFunc(timeout, func() {
		c.cleanupPendingQuery(messageID)
		select {
		case pending.errorChan <- fmt.Errorf("list databases timeout"):
		default:
		}
	})

	c.mutex.Lock()
	c.pendingQueries[messageID] = pending
	c.mutex.Unlock()

	// Send message
	messageBytes, err := message.ToJSON()
	if err != nil {
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	select {
	case c.writeChan <- messageBytes:
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	case <-time.After(c.config.WriteTimeout):
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("write timeout")
	}

	// Wait for result
	select {
	case result := <-pending.resultChan:
		c.cleanupPendingQuery(messageID)
		// Parse databases from the result data
		if result.Success {
			// Extract databases from the Data field
			if result.Data != nil && len(result.Data) > 0 {
				if dbsData, ok := result.Data[0]["databases"]; ok {
					dbBytes, _ := json.Marshal(dbsData)
					var databases []DatabaseInfo
					json.Unmarshal(dbBytes, &databases)
					return databases, nil
				}
			}
			return []DatabaseInfo{}, nil
		}
		return nil, fmt.Errorf("database list failed: %s", result.Error)
	case err := <-pending.errorChan:
		c.cleanupPendingQuery(messageID)
		return nil, err
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	}
}

// executeUserManagementOperation is a helper function for user management operations
func (c *Client) executeUserManagementOperation(message *Message, messageID, operation string) (*UserManagementResult, error) {
	// Setup pending query
	pending := &pendingQuery{
		resultChan: make(chan *SQLResult, 1),
		errorChan:  make(chan error, 1),
	}

	timeout := c.config.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	pending.timeout = time.AfterFunc(timeout, func() {
		c.cleanupPendingQuery(messageID)
		select {
		case pending.errorChan <- fmt.Errorf("%s timeout", operation):
		default:
		}
	})

	c.mutex.Lock()
	c.pendingQueries[messageID] = pending
	c.mutex.Unlock()

	// Send message
	messageBytes, err := message.ToJSON()
	if err != nil {
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	select {
	case c.writeChan <- messageBytes:
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	case <-time.After(c.config.WriteTimeout):
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("write timeout")
	}

	// Wait for result
	select {
	case result := <-pending.resultChan:
		c.cleanupPendingQuery(messageID)
		// Extract message from result data
		var message string
		if result.Data != nil && len(result.Data) > 0 {
			if msgData, ok := result.Data[0]["message"]; ok {
				if msg, ok := msgData.(string); ok {
					message = msg
				}
			}
		}
		return &UserManagementResult{
			Success: result.Success,
			Message: message,
			Error:   result.Error,
		}, nil
	case err := <-pending.errorChan:
		c.cleanupPendingQuery(messageID)
		return nil, err
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	}
}

// Ping sends a ping message to test the connection
func (c *Client) Ping() error {
	if atomic.LoadInt32(&c.connected) == 0 {
		return fmt.Errorf("not connected")
	}

	messageID := c.generateMessageID()
	message := NewMessage("ping")
	message.MessageID = messageID

	messageBytes, err := message.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal ping message: %w", err)
	}

	select {
	case c.writeChan <- messageBytes:
		return nil
	case <-time.After(c.config.WriteTimeout):
		return fmt.Errorf("ping write timeout")
	case <-c.ctx.Done():
		return fmt.Errorf("client is shutting down")
	}
}

// GetHealth retrieves server health status
func (c *Client) GetHealth() (*HealthStatus, error) {
	if c.config.URL == "" {
		return nil, fmt.Errorf("no server URL configured")
	}

	// Convert WebSocket URL to HTTP URL
	u, err := url.Parse(c.config.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	if u.Scheme == "ws" {
		u.Scheme = "http"
	} else if u.Scheme == "wss" {
		u.Scheme = "https"
	}
	u.Path = "/health"
	u.RawQuery = ""

	resp, err := http.Get(u.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get health status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}

	var health HealthStatus
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return nil, fmt.Errorf("failed to decode health response: %w", err)
	}

	return &health, nil
}

// Disconnect closes the connection to the server
func (c *Client) Disconnect() {
	c.cancel()

	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
	}

	// Clear refresh timer
	c.clearRefreshTimer()

	if c.conn != nil {
		c.conn.Close()
	}

	close(c.closeChan)
}

// GetConnectionInfo returns the current connection information
func (c *Client) GetConnectionInfo() *ConnectionInfo {
	return c.connectionInfo
}

// IsConnected returns true if the client is connected
func (c *Client) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// Login authenticates with username and password to obtain JWT tokens
func (c *Client) Login(username, password, database string) (*LoginResponse, error) {
	u, err := url.Parse(c.config.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Change scheme from ws/wss to http/https
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	default:
		return nil, fmt.Errorf("unsupported URL scheme: %s", u.Scheme)
	}

	u.Path = "/login"

	loginReq := LoginRequest{
		Username: username,
		Password: password,
		Database: database,
	}

	reqBody, err := json.Marshal(loginReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal login request: %w", err)
	}

	resp, err := http.Post(u.String(), "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send login request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errorResp map[string]string
		json.NewDecoder(resp.Body).Decode(&errorResp)
		if errMsg, ok := errorResp["error"]; ok {
			return nil, fmt.Errorf("login failed: %s", errMsg)
		}
		return nil, fmt.Errorf("login failed with status: %d", resp.StatusCode)
	}

	var loginResp LoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return nil, fmt.Errorf("failed to decode login response: %w", err)
	}

	// Store token info
	c.tokenInfo = &TokenInfo{
		AccessToken:  loginResp.AccessToken,
		RefreshToken: loginResp.RefreshToken,
		ExpiresAt:    time.Now().Add(time.Duration(loginResp.ExpiresIn) * time.Second),
		TokenType:    loginResp.TokenType,
	}

	// Setup auto-refresh if enabled
	if c.config.AutoRefresh {
		c.setupTokenRefresh()
	}

	return &loginResp, nil
}

// RefreshToken refreshes the access token using the refresh token
func (c *Client) RefreshToken() (*LoginResponse, error) {
	if c.tokenInfo == nil || c.tokenInfo.RefreshToken == "" {
		return nil, fmt.Errorf("no refresh token available")
	}

	if !atomic.CompareAndSwapInt32(&c.isRefreshing, 0, 1) {
		return nil, fmt.Errorf("token refresh already in progress")
	}
	defer atomic.StoreInt32(&c.isRefreshing, 0)

	u, err := url.Parse(c.config.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Change scheme from ws/wss to http/https
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	default:
		return nil, fmt.Errorf("unsupported URL scheme: %s", u.Scheme)
	}

	u.Path = "/refresh"

	refreshReq := RefreshTokenRequest{
		RefreshToken: c.tokenInfo.RefreshToken,
	}

	reqBody, err := json.Marshal(refreshReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal refresh request: %w", err)
	}

	resp, err := http.Post(u.String(), "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send refresh request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errorResp map[string]string
		json.NewDecoder(resp.Body).Decode(&errorResp)
		if errMsg, ok := errorResp["error"]; ok {
			return nil, fmt.Errorf("token refresh failed: %s", errMsg)
		}
		return nil, fmt.Errorf("token refresh failed with status: %d", resp.StatusCode)
	}

	var refreshResp LoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&refreshResp); err != nil {
		return nil, fmt.Errorf("failed to decode refresh response: %w", err)
	}

	// Update token info
	c.tokenInfo = &TokenInfo{
		AccessToken:  refreshResp.AccessToken,
		RefreshToken: refreshResp.RefreshToken,
		ExpiresAt:    time.Now().Add(time.Duration(refreshResp.ExpiresIn) * time.Second),
		TokenType:    refreshResp.TokenType,
	}

	// Setup next refresh
	if c.config.AutoRefresh {
		c.setupTokenRefresh()
	}

	return &refreshResp, nil
}

// GetTokenInfo returns the current token information
func (c *Client) GetTokenInfo() *TokenInfo {
	return c.tokenInfo
}

// SetTokenInfo sets the token information (for when tokens are obtained externally)
func (c *Client) SetTokenInfo(tokenInfo *TokenInfo) {
	c.tokenInfo = tokenInfo
	if c.config.AutoRefresh {
		c.setupTokenRefresh()
	}
}

// ClearTokens clears stored tokens
func (c *Client) ClearTokens() {
	c.tokenInfo = nil
	c.clearRefreshTimer()
}

// StartBackup initiates a backup operation
// Requires BACKUP privilege
func (c *Client) StartBackup(options BackupOptions) (*BackupResult, error) {
	if atomic.LoadInt32(&c.connected) == 0 {
		return nil, fmt.Errorf("not connected to database")
	}

	messageID := c.generateMessageID()

	// Create message
	message := NewMessage("backup_start")
	message.MessageID = messageID
	message.SetData("backup", map[string]interface{}{
		"description": options.Description,
		"tags":        options.Tags,
	})

	// Setup pending query
	pending := &pendingQuery{
		resultChan: make(chan *SQLResult, 1),
		errorChan:  make(chan error, 1),
	}

	timeout := c.config.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Minute // Backup operations can take longer
	}

	pending.timeout = time.AfterFunc(timeout, func() {
		c.cleanupPendingQuery(messageID)
		select {
		case pending.errorChan <- fmt.Errorf("backup operation timeout"):
		default:
		}
	})

	c.mutex.Lock()
	c.pendingQueries[messageID] = pending
	c.mutex.Unlock()

	// Send message
	messageBytes, err := message.ToJSON()
	if err != nil {
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	select {
	case c.writeChan <- messageBytes:
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	case <-time.After(c.config.WriteTimeout):
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("write timeout")
	}

	// Wait for completion
	select {
	case result := <-pending.resultChan:
		c.cleanupPendingQuery(messageID)

		// Parse backup info from result
		backupResult := &BackupResult{
			Success: result.Success,
			Error:   result.Error,
		}

		if result.Success && result.Data != nil && len(result.Data) > 0 {
			if backupInfoData, ok := result.Data[0]["backup_info"]; ok {
				backupBytes, _ := json.Marshal(backupInfoData)
				var backupInfo BackupInfo
				if err := json.Unmarshal(backupBytes, &backupInfo); err == nil {
					backupResult.BackupInfo = &backupInfo
				}
			}
			if message, ok := result.Data[0]["message"]; ok {
				if msg, ok := message.(string); ok {
					backupResult.Message = msg
				}
			}
		}

		return backupResult, nil
	case err := <-pending.errorChan:
		c.cleanupPendingQuery(messageID)
		return nil, err
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	}
}

// StartRestore initiates a restore operation
// Requires RESTORE privilege
func (c *Client) StartRestore(options RestoreOptions) (*RestoreResult, error) {
	if atomic.LoadInt32(&c.connected) == 0 {
		return nil, fmt.Errorf("not connected to database")
	}

	messageID := c.generateMessageID()

	// Create message
	message := NewMessage("restore_start")
	message.MessageID = messageID
	message.SetData("restore", map[string]interface{}{
		"backup_filename":  options.BackupFilename,
		"force_restore":    options.ForceRestore,
		"verify_integrity": options.VerifyIntegrity,
	})

	// Setup pending query
	pending := &pendingQuery{
		resultChan: make(chan *SQLResult, 1),
		errorChan:  make(chan error, 1),
	}

	timeout := c.config.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Minute // Restore operations can take longer
	}

	pending.timeout = time.AfterFunc(timeout, func() {
		c.cleanupPendingQuery(messageID)
		select {
		case pending.errorChan <- fmt.Errorf("restore operation timeout"):
		default:
		}
	})

	c.mutex.Lock()
	c.pendingQueries[messageID] = pending
	c.mutex.Unlock()

	// Send message
	messageBytes, err := message.ToJSON()
	if err != nil {
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	select {
	case c.writeChan <- messageBytes:
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	case <-time.After(c.config.WriteTimeout):
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("write timeout")
	}

	// Wait for completion
	select {
	case result := <-pending.resultChan:
		c.cleanupPendingQuery(messageID)

		// Parse restore result
		restoreResult := &RestoreResult{
			Success: result.Success,
			Error:   result.Error,
		}

		if result.Success && result.Data != nil && len(result.Data) > 0 {
			if message, ok := result.Data[0]["message"]; ok {
				if msg, ok := message.(string); ok {
					restoreResult.Message = msg
				}
			}
		}

		return restoreResult, nil
	case err := <-pending.errorChan:
		c.cleanupPendingQuery(messageID)
		return nil, err
	case <-c.ctx.Done():
		c.cleanupPendingQuery(messageID)
		return nil, fmt.Errorf("client is shutting down")
	}
}

// Private methods

func (c *Client) buildConnectionURL() (string, error) {
	u, err := url.Parse(c.config.URL)
	if err != nil {
		return "", err
	}

	// JWT-only authentication - no URL parameters needed
	return u.String(), nil
}

func (c *Client) waitForAuth() (*ConnectionInfo, error) {
	// Set a read deadline for authentication
	c.conn.SetReadDeadline(time.Now().Add(c.config.Timeout))
	defer c.conn.SetReadDeadline(time.Time{})

	for {
		_, messageBytes, err := c.conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("failed to read auth response: %w", err)
		}

		message, err := MessageFromJSON(messageBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse auth message: %w", err)
		}

		switch message.Type {
		case "auth_ok":
			info := &ConnectionInfo{
				ConnectionID:  message.ConnectionID,
				Database:      message.Database,
				ServerVersion: message.ServerVersion,
			}
			if version, ok := message.GetData("server_version"); ok {
				if s, ok := version.(string); ok {
					info.ServerVersion = s
				}
			}
			return info, nil

		case "auth_error":
			return nil, fmt.Errorf("authentication failed: %s", message.Error)

		default:
			// Ignore other messages during auth
			continue
		}
	}
}

func (c *Client) readLoop() {
	defer c.handleDisconnection("read loop ended")

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
		_, messageBytes, err := c.conn.ReadMessage()
		if err != nil {
			if c.onError != nil {
				c.onError(fmt.Sprintf("read error: %v", err), nil)
			}
			return
		}

		message, err := MessageFromJSON(messageBytes)
		if err != nil {
			if c.onError != nil {
				c.onError(fmt.Sprintf("failed to parse message: %v", err), nil)
			}
			continue
		}

		c.handleMessage(message)
	}
}

func (c *Client) writeLoop() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.closeChan:
			return
		case message := <-c.writeChan:
			c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				if c.onError != nil {
					c.onError(fmt.Sprintf("write error: %v", err), nil)
				}
				return
			}
		case <-ticker.C:
			// Send ping to keep connection alive
			c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) pingLoop() {
	if c.config.PingInterval <= 0 {
		return
	}

	ticker := time.NewTicker(c.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.closeChan:
			return
		case <-ticker.C:
			if err := c.Ping(); err != nil {
				// Ping failed, connection might be dead
				return
			}
		}
	}
}

func (c *Client) handleMessage(message *Message) {
	// Emit generic message event
	if c.onMessage != nil {
		c.onMessage(message)
	}

	switch message.Type {
	case "sql_result":
		c.handleSQLResult(message)
	case "user_create_result", "user_drop_result", "user_grant_result", "user_revoke_result":
		c.handleUserManagementResult(message)
	case "user_list_result":
		c.handleUserListResult(message)
	case "database_list_result":
		c.handleDatabaseListResult(message)
	case "database_create_result", "database_drop_result":
		c.handleDatabaseManagementResult(message)
	case "database_info_result":
		c.handleDatabaseInfoResult(message)
	case "backup_started", "backup_complete", "backup_error":
		c.handleBackupResult(message)
	case "backup_progress":
		c.handleBackupProgress(message)
	case "restore_started", "restore_complete", "restore_error":
		c.handleRestoreResult(message)
	case "restore_progress":
		c.handleRestoreProgress(message)
	case "maintenance":
		c.handleMaintenanceStatus(message)
	case "pong":
		if c.onPong != nil && message.MessageID != "" {
			timestamp := ""
			if ts, ok := message.GetData("timestamp"); ok {
				if s, ok := ts.(string); ok {
					timestamp = s
				}
			}
			c.onPong(message.MessageID, timestamp)
		}
	case "error":
		if c.onError != nil {
			c.onError(message.Error, &message.MessageID)
		}
		if message.MessageID != "" {
			c.rejectPendingQuery(message.MessageID, fmt.Errorf(message.Error))
		}
	}
}

func (c *Client) handleSQLResult(message *Message) {
	result, ok := message.GetData("result")
	if !ok {
		return
	}

	// Convert result to SQLResult
	resultBytes, err := json.Marshal(result)
	if err != nil {
		return
	}

	var sqlResult SQLResult
	if err := json.Unmarshal(resultBytes, &sqlResult); err != nil {
		return
	}

	// Extract additional data
	sql, _ := message.GetData("sql")
	database, _ := message.GetData("database")
	executionID, _ := message.GetData("execution_id")

	sqlStr, _ := sql.(string)
	dbStr, _ := database.(string)
	execStr, _ := executionID.(string)

	// Emit SQL result event
	if c.onSQLResult != nil {
		c.onSQLResult(&sqlResult, execStr, sqlStr, dbStr)
	}

	// Resolve pending query
	if message.MessageID != "" {
		c.resolvePendingQuery(message.MessageID, &sqlResult)
	}
}

func (c *Client) handleUserManagementResult(message *Message) {
	// Create SQLResult from message data for compatibility with pending query system
	sqlResult := &SQLResult{
		Success: false,
		Error:   message.Error,
	}

	// Extract success and message from data
	if success, ok := message.GetData("success"); ok {
		if s, ok := success.(bool); ok {
			sqlResult.Success = s
		}
	}

	if sqlResult.Success {
		if msg, ok := message.GetData("message"); ok {
			if m, ok := msg.(string); ok {
				sqlResult.Data = []map[string]interface{}{{"message": m}}
			}
		}
	}

	// Resolve pending query
	if message.MessageID != "" {
		c.resolvePendingQuery(message.MessageID, sqlResult)
	}
}

func (c *Client) handleUserListResult(message *Message) {
	// Create UserListResult from message data
	userListResult := &UserListResult{
		Success: false,
		Error:   message.Error,
	}

	// Extract success from data
	if success, ok := message.GetData("success"); ok {
		if s, ok := success.(bool); ok {
			userListResult.Success = s
		}
	}

	if userListResult.Success {
		// Extract users and count data directly from the message
		if usersData, ok := message.GetData("users"); ok {
			userBytes, _ := json.Marshal(usersData)
			json.Unmarshal(userBytes, &userListResult.Users)
		}
		if countData, ok := message.GetData("count"); ok {
			if count, ok := countData.(float64); ok {
				userListResult.Count = int(count)
			}
		}
	}

	// For now, we will adapt this to the pending query system by wrapping it
	// A better long-term solution would be a dedicated channel for this type
	sqlResult := &SQLResult{
		Success: userListResult.Success,
		Error:   userListResult.Error,
		Data: []map[string]interface{}{
			{
				"users": userListResult.Users,
				"count": userListResult.Count,
			},
		},
	}

	// Resolve pending query
	if message.MessageID != "" {
		c.resolvePendingQuery(message.MessageID, sqlResult)
	}
}

func (c *Client) handleDatabaseListResult(message *Message) {
	// Create DatabaseListResult from message data
	databaseListResult := &DatabaseListResult{
		Success: false,
		Error:   message.Error,
	}

	// Extract success from data
	if success, ok := message.GetData("success"); ok {
		if s, ok := success.(bool); ok {
			databaseListResult.Success = s
		}
	}

	if databaseListResult.Success {
		// Extract databases and count data directly from the message
		if databasesData, ok := message.GetData("databases"); ok {
			databaseBytes, _ := json.Marshal(databasesData)
			json.Unmarshal(databaseBytes, &databaseListResult.Databases)
		}
		if countData, ok := message.GetData("count"); ok {
			if count, ok := countData.(float64); ok {
				databaseListResult.Count = int(count)
			}
		}
	}

	// Adapt to the pending query system by wrapping it
	sqlResult := &SQLResult{
		Success: databaseListResult.Success,
		Error:   databaseListResult.Error,
		Data: []map[string]interface{}{
			{
				"databases": databaseListResult.Databases,
				"count":     databaseListResult.Count,
			},
		},
	}

	// Resolve pending query
	if message.MessageID != "" {
		c.resolvePendingQuery(message.MessageID, sqlResult)
	}
}

func (c *Client) handleBackupResult(message *Message) {
	// Only resolve pending query for backup_complete and backup_error messages
	// backup_started is just an acknowledgment that the backup process has begun
	if message.Type == "backup_complete" || message.Type == "backup_error" {
		// Create SQLResult from message data for compatibility with pending query system
		sqlResult := &SQLResult{
			Success: message.Type == "backup_complete",
			Error:   message.Error,
		}

		if message.Type == "backup_complete" {
			// Extract backup info and message
			data := make(map[string]interface{})
			if backupInfo, ok := message.GetData("backup_info"); ok {
				data["backup_info"] = backupInfo
			}
			if msg, ok := message.GetData("message"); ok {
				data["message"] = msg
			}
			sqlResult.Data = []map[string]interface{}{data}
		}

		// Resolve pending query
		if message.MessageID != "" {
			c.resolvePendingQuery(message.MessageID, sqlResult)
		}
	}
	// backup_started messages are just acknowledgments and don't resolve the pending query
}

func (c *Client) handleBackupProgress(message *Message) {
	if c.onBackupProgress != nil {
		if progressData, ok := message.GetData("progress"); ok {
			progressBytes, _ := json.Marshal(progressData)
			var progress BackupProgress
			if err := json.Unmarshal(progressBytes, &progress); err == nil {
				c.onBackupProgress(&progress)
			}
		}
	}
}

func (c *Client) handleRestoreResult(message *Message) {
	// Create SQLResult from message data for compatibility with pending query system
	sqlResult := &SQLResult{
		Success: message.Type == "restore_complete" || message.Type == "restore_started",
		Error:   message.Error,
	}

	// For restore_complete, extract message
	if message.Type == "restore_complete" {
		data := make(map[string]interface{})
		if msg, ok := message.GetData("message"); ok {
			data["message"] = msg
		}
		sqlResult.Data = []map[string]interface{}{data}
	}

	// Resolve pending query
	if message.MessageID != "" {
		c.resolvePendingQuery(message.MessageID, sqlResult)
	}
}

func (c *Client) handleRestoreProgress(message *Message) {
	if c.onRestoreProgress != nil {
		if progressData, ok := message.GetData("progress"); ok {
			progressBytes, _ := json.Marshal(progressData)
			var progress RestoreProgress
			if err := json.Unmarshal(progressBytes, &progress); err == nil {
				c.onRestoreProgress(&progress)
			}
		}
	}
}

func (c *Client) handleMaintenanceStatus(message *Message) {
	if c.onMaintenance != nil {
		status := &MaintenanceStatus{}
		if data, ok := message.GetData("state"); ok {
			if s, ok := data.(string); ok {
				status.State = s
			}
		}
		if data, ok := message.GetData("message"); ok {
			if s, ok := data.(string); ok {
				status.Message = s
			}
		}
		if data, ok := message.GetData("started_at"); ok {
			if s, ok := data.(string); ok {
				if t, err := time.Parse(time.RFC3339, s); err == nil {
					status.StartedAt = t
				}
			}
		}
		if data, ok := message.GetData("estimated_completion_ms"); ok {
			if f, ok := data.(float64); ok {
				status.EstimatedCompletion = int64(f)
			}
		}
		if data, ok := message.GetData("progress"); ok {
			if f, ok := data.(float64); ok {
				status.Progress = f
			}
		}
		c.onMaintenance(status)
	}
}

func (c *Client) handleDatabaseManagementResult(message *Message) {
	if message.MessageID != "" {
		result := &DatabaseManagementResult{
			Success: !message.HasError(),
			Error:   message.Error,
		}
		
		if data, ok := message.GetData("database_name"); ok {
			if s, ok := data.(string); ok {
				result.DatabaseName = s
			}
		}
		if data, ok := message.GetData("message"); ok {
			if s, ok := data.(string); ok {
				result.Message = s
			}
		}
		
		if result.Success {
			c.resolvePendingQuery(message.MessageID, &SQLResult{
				Success: true,
				Data:    []map[string]interface{}{{"result": result}},
			})
		} else {
			c.rejectPendingQuery(message.MessageID, fmt.Errorf(result.Error))
		}
	}
}

func (c *Client) handleDatabaseInfoResult(message *Message) {
	if message.MessageID != "" {
		result := &DatabaseInfoResult{
			Success: !message.HasError(),
			Error:   message.Error,
		}
		
		if data, ok := message.GetData("database_name"); ok {
			if s, ok := data.(string); ok {
				result.DatabaseName = s
			}
		}
		if data, ok := message.GetData("stats"); ok {
			if stats, ok := data.(map[string]interface{}); ok {
				result.Stats = stats
			}
		}
		
		if result.Success {
			c.resolvePendingQuery(message.MessageID, &SQLResult{
				Success: true,
				Data:    []map[string]interface{}{{"result": result}},
			})
		} else {
			c.rejectPendingQuery(message.MessageID, fmt.Errorf(result.Error))
		}
	}
}

func (c *Client) resolvePendingQuery(messageID string, result *SQLResult) {
	c.mutex.Lock()
	pending, exists := c.pendingQueries[messageID]
	if exists {
		delete(c.pendingQueries, messageID)
	}
	c.mutex.Unlock()

	if exists {
		if pending.timeout != nil {
			pending.timeout.Stop()
		}

		if result.Success {
			select {
			case pending.resultChan <- result:
			default:
			}
		} else {
			select {
			case pending.errorChan <- fmt.Errorf(result.Error):
			default:
			}
		}
	}
}

func (c *Client) rejectPendingQuery(messageID string, err error) {
	c.mutex.Lock()
	pending, exists := c.pendingQueries[messageID]
	if exists {
		delete(c.pendingQueries, messageID)
	}
	c.mutex.Unlock()

	if exists {
		if pending.timeout != nil {
			pending.timeout.Stop()
		}
		select {
		case pending.errorChan <- err:
		default:
		}
	}
}

func (c *Client) cleanupPendingQuery(messageID string) {
	c.mutex.Lock()
	pending, exists := c.pendingQueries[messageID]
	if exists {
		delete(c.pendingQueries, messageID)
	}
	c.mutex.Unlock()

	if exists && pending.timeout != nil {
		pending.timeout.Stop()
	}
}

func (c *Client) handleDisconnection(reason string) {
	wasConnected := atomic.SwapInt32(&c.connected, 0) == 1

	// Clean up all pending queries
	c.mutex.Lock()
	for _, pending := range c.pendingQueries {
		if pending.timeout != nil {
			pending.timeout.Stop()
		}
		select {
		case pending.errorChan <- fmt.Errorf("connection lost"):
		default:
		}
	}
	c.pendingQueries = make(map[string]*pendingQuery)
	c.mutex.Unlock()

	if wasConnected {
		if c.onDisconnected != nil {
			c.onDisconnected(reason)
		}

		// Auto-reconnect if enabled
		if c.config.Reconnect && c.reconnectAttempts < c.config.MaxReconnectAttempts {
			c.scheduleReconnect()
		}
	}
}

func (c *Client) scheduleReconnect() {
	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
	}

	c.reconnectTimer = time.AfterFunc(c.config.ReconnectInterval, func() {
		c.reconnectAttempts++
		if _, err := c.Connect(); err != nil {
			// Reconnection failed, schedule another attempt if we have attempts left
			if c.reconnectAttempts < c.config.MaxReconnectAttempts {
				c.scheduleReconnect()
			}
		}
	})
}

func (c *Client) generateMessageID() string {
	counter := atomic.AddInt64(&c.messageCounter, 1)
	return fmt.Sprintf("msg_%d_%d", counter, time.Now().UnixNano())
}

func (c *Client) setupTokenRefresh() {
	c.clearRefreshTimer()

	if c.tokenInfo == nil || !c.config.AutoRefresh {
		return
	}

	timeToRefresh := time.Until(c.tokenInfo.ExpiresAt) - c.config.RefreshThreshold

	if timeToRefresh > 0 {
		c.refreshTimer = time.AfterFunc(timeToRefresh, func() {
			if _, err := c.RefreshToken(); err != nil {
				// Token refresh failed, emit error
				if c.onError != nil {
					c.onError("Token refresh failed: "+err.Error(), nil)
				}
			}
		})
	}
}

func (c *Client) clearRefreshTimer() {
	if c.refreshTimer != nil {
		c.refreshTimer.Stop()
		c.refreshTimer = nil
	}
}
