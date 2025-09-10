package kahflane

import (
	"encoding/json"
	"time"
)

// Message represents a message exchanged between client and server
// Corresponds to server's message.go Message struct
type Message struct {
	Type          string                 `json:"type"`
	MessageID     string                 `json:"message_id,omitempty"`
	Username      string                 `json:"username,omitempty"`
	Host          string                 `json:"host,omitempty"`
	Database      string                 `json:"database,omitempty"`
	ConnectionID  string                 `json:"connection_id,omitempty"`
	Timestamp     time.Time              `json:"timestamp"`
	Data          map[string]interface{} `json:"data,omitempty"`
	Error         string                 `json:"error,omitempty"`
	ServerVersion string                 `json:"server_version,omitempty"`
}

// SQLCommand represents SQL command data sent to server
// Corresponds to server's message.go SQLCommand struct
type SQLCommand struct {
	SQL        string        `json:"sql"`
	Parameters []interface{} `json:"parameters,omitempty"`
	Database   string        `json:"database,omitempty"`
}

// ColumnInfo represents database column metadata
// Corresponds to server's message.go ColumnInfo struct
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Key      string `json:"key,omitempty"`
	Default  string `json:"default,omitempty"`
	Extra    string `json:"extra,omitempty"`
}

// SQLResult represents the result of a SQL query execution
// Corresponds to server's message.go SQLResult struct
type SQLResult struct {
	Success       bool                     `json:"success"`
	Data          []map[string]interface{} `json:"data,omitempty"`
	Columns       []ColumnInfo             `json:"columns,omitempty"`
	RowsAffected  int64                    `json:"rows_affected"`
	LastInsertID  int64                    `json:"last_insert_id,omitempty"`
	ExecutionTime float64                  `json:"execution_time_ms"`
	Warnings      []string                 `json:"warnings,omitempty"`
	Error         string                   `json:"error,omitempty"`
}

// ConnectionInfo represents server connection information
type ConnectionInfo struct {
	ConnectionID  string `json:"connection_id"`
	Database      string `json:"database"`
	ServerVersion string `json:"server_version"`
}

// HealthStatus represents server health information
type HealthStatus struct {
	Status      string `json:"status"`
	ServerType  string `json:"server_type"`
	Version     string `json:"version"`
	Connections int    `json:"connections"`
}

// ConnectionConfig holds configuration for database connection
type ConnectionConfig struct {
	URL                  string        // WebSocket URL (e.g., "ws://localhost:8080")
	Timeout              time.Duration // Connection timeout (default: 30s)
	ReadTimeout          time.Duration // Read timeout for messages (default: 60s)
	WriteTimeout         time.Duration // Write timeout for messages (default: 10s)
	PingInterval         time.Duration // Interval for ping messages (default: 30s)
	Reconnect            bool          // Enable auto-reconnect (default: true)
	ReconnectInterval    time.Duration // Reconnect interval (default: 5s)
	MaxReconnectAttempts int           // Max reconnect attempts (default: 10)

	// JWT Authentication (required)
	AccessToken      string        // JWT access token
	RefreshToken     string        // JWT refresh token
	AutoRefresh      bool          // Automatically refresh tokens when needed
	RefreshThreshold time.Duration // Time before expiry to refresh token (default: 5 minutes)
}

// QueryOptions holds options for query execution
type QueryOptions struct {
	Database   string        // Override default database
	Parameters []interface{} // Query parameters
	Timeout    time.Duration // Query timeout
}

// DefaultConnectionConfig returns a connection config with sensible defaults
func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		Timeout:              30 * time.Second,
		ReadTimeout:          60 * time.Second,
		WriteTimeout:         10 * time.Second,
		PingInterval:         30 * time.Second,
		Reconnect:            true,
		ReconnectInterval:    5 * time.Second,
		MaxReconnectAttempts: 10,
		AutoRefresh:          true,
		RefreshThreshold:     5 * time.Minute,
	}
}

// NewMessage creates a new message with the given type
func NewMessage(messageType string) *Message {
	return &Message{
		Type:      messageType,
		Timestamp: time.Now(),
		Data:      make(map[string]interface{}),
	}
}

// SetData sets a data field in the message
func (m *Message) SetData(key string, value interface{}) {
	if m.Data == nil {
		m.Data = make(map[string]interface{})
	}
	m.Data[key] = value
}

// GetData gets a data field from the message
func (m *Message) GetData(key string) (interface{}, bool) {
	if m.Data == nil {
		return nil, false
	}
	value, exists := m.Data[key]
	return value, exists
}

// ToJSON converts the message to JSON bytes
func (m *Message) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

// MessageFromJSON parses a message from JSON bytes
func MessageFromJSON(data []byte) (*Message, error) {
	var message Message
	err := json.Unmarshal(data, &message)
	return &message, err
}

// SetError sets an error message
func (m *Message) SetError(err string) {
	m.Error = err
}

// HasError returns true if the message contains an error
func (m *Message) HasError() bool {
	return m.Error != ""
}

// User Management Types (corresponding to server auth.go)

// CreateUserRequest represents a request to create a new database user
type CreateUserRequest struct {
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

// DropUserRequest represents a request to drop a database user
type DropUserRequest struct {
	Username string `json:"username"`
}

// GrantPrivilegeRequest represents a request to grant privilege to a user
type GrantPrivilegeRequest struct {
	Username  string `json:"username"`
	Host      string `json:"host,omitempty"`
	Database  string `json:"database"`
	Table     string `json:"table,omitempty"`
	Privilege string `json:"privilege"`
}

// RevokePrivilegeRequest represents a request to revoke privilege from a user
type RevokePrivilegeRequest struct {
	Username  string `json:"username"`
	Database  string `json:"database"`
	Table     string `json:"table,omitempty"`
	Privilege string `json:"privilege"`
}

// UserInfo represents information about a database user
type UserInfo struct {
	Username           string          `json:"username"`
	Host               string          `json:"host"`
	CreatedAt          string          `json:"created_at"`
	MaxConnections     int             `json:"max_connections"`
	CurrentConnections int             `json:"current_connections"`
	Databases          map[string]bool `json:"databases"`
}

// UserManagementResult represents the result of a user management operation
type UserManagementResult struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// UserListResult represents the result of listing users
type UserListResult struct {
	Success bool       `json:"success"`
	Users   []UserInfo `json:"users"`
	Count   int        `json:"count"`
	Error   string     `json:"error,omitempty"`
}

// JWT Authentication Types

// LoginRequest represents a login request to obtain JWT tokens
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"` // Required field
}

// LoginResponse represents a successful login response with JWT tokens
type LoginResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

// RefreshTokenRequest represents a token refresh request
type RefreshTokenRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// TokenInfo holds JWT token information
type TokenInfo struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
	TokenType    string
}

// Backup and Restore Types (corresponding to server message.go)

// BackupStartRequest represents a request to start a backup operation
type BackupStartRequest struct {
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

// BackupProgress represents the progress of a backup operation
type BackupProgress struct {
	Stage              string  `json:"stage"`      // "exporting", "compressing", "uploading", "finalizing"
	Percentage         float64 `json:"percentage"` // 0-100
	BytesTotal         int64   `json:"bytes_total"`
	BytesProcessed     int64   `json:"bytes_processed"`
	EstimatedRemaining int64   `json:"estimated_remaining_ms"`
	CurrentFile        string  `json:"current_file,omitempty"`
	Message            string  `json:"message,omitempty"`
}

// BackupInfo represents information about a backup
type BackupInfo struct {
	ID               string    `json:"id"`
	Filename         string    `json:"filename"`
	Description      string    `json:"description,omitempty"`
	Tags             []string  `json:"tags,omitempty"`
	SizeBytes        int64     `json:"size_bytes"`
	CreatedAt        time.Time `json:"created_at"`
	CreatedBy        string    `json:"created_by"`
	DatabaseVersion  string    `json:"database_version"`
	CompressionLevel int       `json:"compression_level"`
	Checksum         string    `json:"checksum"`
	Status           string    `json:"status"` // "in_progress", "completed", "failed"
}

// RestoreStartRequest represents a request to start a restore operation
type RestoreStartRequest struct {
	BackupFilename  string `json:"backup_filename"`
	ForceRestore    bool   `json:"force_restore,omitempty"`
	VerifyIntegrity bool   `json:"verify_integrity,omitempty"`
}

// RestoreProgress represents the progress of a restore operation
type RestoreProgress struct {
	Stage              string  `json:"stage"`      // "downloading", "verifying", "restoring", "finalizing"
	Percentage         float64 `json:"percentage"` // 0-100
	BytesTotal         int64   `json:"bytes_total"`
	BytesProcessed     int64   `json:"bytes_processed"`
	EstimatedRemaining int64   `json:"estimated_remaining_ms"`
	CurrentFile        string  `json:"current_file,omitempty"`
	Message            string  `json:"message,omitempty"`
}

// MaintenanceStatus represents the server's maintenance status
type MaintenanceStatus struct {
	State               string    `json:"state"`
	Message             string    `json:"message"`
	StartedAt           time.Time `json:"started_at"`
	EstimatedCompletion int64     `json:"estimated_completion_ms,omitempty"`
	Progress            float64   `json:"progress,omitempty"`
}

// BackupResult represents the result of a backup operation
type BackupResult struct {
	Success    bool        `json:"success"`
	BackupInfo *BackupInfo `json:"backup_info,omitempty"`
	Message    string      `json:"message,omitempty"`
	Error      string      `json:"error,omitempty"`
}

// RestoreResult represents the result of a restore operation
type RestoreResult struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// BackupOptions holds options for backup operations
type BackupOptions struct {
	Description string   // Optional description for the backup
	Tags        []string // Optional tags for the backup
}

// RestoreOptions holds options for restore operations
type RestoreOptions struct {
	BackupFilename  string // Required: filename of the backup to restore
	ForceRestore    bool   // Optional: force restore even if risky
	VerifyIntegrity bool   // Optional: verify backup integrity before restore
}
