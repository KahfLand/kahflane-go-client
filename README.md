# KahfLane Go Client

A Go client library for connecting to KahfLane database servers via WebSocket connections. This library provides both single connection and connection pool management with comprehensive features including authentication, query execution, user management, backup/restore operations, and real-time event handling.

## Features

- **WebSocket-based Communication**: Real-time bidirectional communication with the server
- **JWT Authentication**: Secure authentication with automatic token refresh
- **Connection Pooling**: Efficient connection management with configurable pool settings
- **SQL Query Execution**: Execute queries with parameters and get structured results
- **User Management**: Create, drop users and manage privileges
- **Backup & Restore**: Database backup and restore operations with progress tracking
- **Event Handling**: Real-time event callbacks for connection status, errors, and progress updates
- **Auto-Reconnection**: Automatic reconnection with configurable retry logic
- **Health Monitoring**: Server health checks and connection status monitoring

## Installation

```bash
go get github.com/KahfLand/kahflane/client-go
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"
    "time"
    
    kahflane "github.com/KahfLand/kahflane/client-go"
)

func main() {
    // Create client with configuration
    client := kahflane.NewClient(kahflane.ConnectionConfig{
        URL: "ws://localhost:8080",
    })
    
    // Login and connect
    _, err := client.Login("username", "password", "database")
    if err != nil {
        log.Fatal(err)
    }
    
    _, err = client.Connect()
    if err != nil {
        log.Fatal(err)
    }
    defer client.Disconnect()
    
    // Execute a query
    result, err := client.Query("SELECT * FROM users")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Query result: %+v\n", result.Data)
}
```

## Configuration

### ConnectionConfig

```go
type ConnectionConfig struct {
    URL                  string        // WebSocket URL (e.g., "ws://localhost:8080")
    Timeout              time.Duration // Connection timeout (default: 30s)
    ReadTimeout          time.Duration // Read timeout for messages (default: 60s)
    WriteTimeout         time.Duration // Write timeout for messages (default: 10s)
    PingInterval         time.Duration // Interval for ping messages (default: 30s)
    Reconnect            bool          // Enable auto-reconnect (default: true)
    ReconnectInterval    time.Duration // Reconnect interval (default: 5s)
    MaxReconnectAttempts int           // Max reconnect attempts (default: 10)
    
    // JWT Authentication
    AccessToken      string        // JWT access token
    RefreshToken     string        // JWT refresh token
    AutoRefresh      bool          // Automatically refresh tokens when needed
    RefreshThreshold time.Duration // Time before expiry to refresh token (default: 5 minutes)
}
```

### Default Configuration

```go
config := kahflane.DefaultConnectionConfig()
config.URL = "ws://localhost:8080"
client := kahflane.NewClient(config)
```

## Authentication

### Login with Username/Password

```go
loginResp, err := client.Login("username", "password", "database")
if err != nil {
    log.Fatal(err)
}

// Tokens are automatically stored in the client
fmt.Printf("Access token expires in: %d seconds\n", loginResp.ExpiresIn)
```

### Token Management

```go
// Manual token refresh
refreshResp, err := client.RefreshToken()
if err != nil {
    log.Fatal(err)
}

// Get current token info
tokenInfo := client.GetTokenInfo()
fmt.Printf("Token expires at: %v\n", tokenInfo.ExpiresAt)

// Clear stored tokens
client.ClearTokens()
```

## Event Handling

```go
client.SetEventHandlers(map[string]interface{}{
    "connected": func(info kahflane.ConnectionInfo) {
        fmt.Printf("Connected to server %s\n", info.ServerVersion)
    },
    "disconnected": func(reason string) {
        fmt.Printf("Disconnected: %s\n", reason)
    },
    "error": func(err string, messageID *string) {
        fmt.Printf("Error: %s\n", err)
    },
    "sql_result": func(result *kahflane.SQLResult, executionID, sql, database string) {
        fmt.Printf("Query completed: %s\n", sql)
    },
    "backup_progress": func(progress *kahflane.BackupProgress) {
        fmt.Printf("Backup: %s %.1f%%\n", progress.Stage, progress.Percentage)
    },
    "restore_progress": func(progress *kahflane.RestoreProgress) {
        fmt.Printf("Restore: %s %.1f%%\n", progress.Stage, progress.Percentage)
    },
})
```

## Query Execution

### Simple Query

```go
result, err := client.Query("SELECT * FROM users")
if err != nil {
    log.Fatal(err)
}

for _, row := range result.Data {
    fmt.Printf("User: %v\n", row)
}
```

### Query with Parameters

```go
result, err := client.Execute(
    "SELECT * FROM users WHERE age > ? AND city = ?",
    []interface{}{25, "New York"},
)
if err != nil {
    log.Fatal(err)
}
```

### Query with Options

```go
result, err := client.Query(
    "SELECT * FROM large_table",
    &kahflane.QueryOptions{
        Database: "analytics",
        Timeout:  5 * time.Minute,
    },
)
```

## Connection Pool

For high-concurrency applications, use the connection pool:

```go
// Create pool configuration
poolConfig := kahflane.DefaultPoolConfig()
poolConfig.URL = "ws://localhost:8080"
poolConfig.MaxConnections = 20
poolConfig.MinConnections = 5

// Create pool
pool, err := kahflane.NewPool(poolConfig)
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

// Use pool for queries
ctx := context.Background()
result, err := pool.Query(ctx, "SELECT * FROM users")
if err != nil {
    log.Fatal(err)
}

// Get pool statistics
stats := pool.Stats()
fmt.Printf("Active connections: %v\n", stats["active_connections"])
```

## User Management

### Create User

```go
result, err := client.CreateUser(&kahflane.CreateUserRequest{
    Username: "newuser",
    Password: "password123",
})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("User created: %s\n", result.Message)
```

### Grant Privileges

```go
result, err := client.GrantPrivilege(&kahflane.GrantPrivilegeRequest{
    Username:  "newuser",
    Database:  "testdb",
    Privilege: "SELECT",
})
if err != nil {
    log.Fatal(err)
}
```

### List Users

```go
result, err := client.ListUsers()
if err != nil {
    log.Fatal(err)
}

for _, user := range result.Users {
    fmt.Printf("User: %s, Databases: %v\n", user.Username, user.Databases)
}
```

## Backup and Restore

### Start Backup

```go
backupResult, err := client.StartBackup(kahflane.BackupOptions{
    Description: "Daily backup",
    Tags:        []string{"daily", "automated"},
})
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Backup started: %s\n", backupResult.BackupInfo.ID)
```

### Start Restore

```go
restoreResult, err := client.StartRestore(kahflane.RestoreOptions{
    BackupFilename:  "backup_20240101.db",
    VerifyIntegrity: true,
    ForceRestore:    false,
})
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Restore started: %s\n", restoreResult.Message)
```

## Health Monitoring

```go
// Check server health
health, err := client.GetHealth()
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Server status: %s, Connections: %d\n", health.Status, health.Connections)

// Ping server
err = client.Ping()
if err != nil {
    log.Printf("Ping failed: %v\n", err)
}

// Check connection status
if client.IsConnected() {
    info := client.GetConnectionInfo()
    fmt.Printf("Connected to database: %s\n", info.Database)
}
```

## Error Handling

```go
result, err := client.Query("SELECT * FROM nonexistent_table")
if err != nil {
    log.Printf("Query error: %v\n", err)
    return
}

if !result.Success {
    log.Printf("Query failed: %s\n", result.Error)
    return
}

if len(result.Warnings) > 0 {
    for _, warning := range result.Warnings {
        log.Printf("Warning: %s\n", warning)
    }
}
```

## Data Types

### SQLResult

```go
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
```

### ColumnInfo

```go
type ColumnInfo struct {
    Name     string `json:"name"`
    Type     string `json:"type"`
    Nullable bool   `json:"nullable"`
    Key      string `json:"key,omitempty"`
    Default  string `json:"default,omitempty"`
    Extra    string `json:"extra,omitempty"`
}
```

## Best Practices

1. **Use Connection Pools**: For applications with multiple concurrent operations
2. **Handle Reconnections**: Enable auto-reconnect for production environments
3. **Set Timeouts**: Configure appropriate timeouts for your use case
4. **Monitor Events**: Use event handlers to track connection status and errors
5. **Secure Tokens**: Store JWT tokens securely and enable auto-refresh
6. **Error Handling**: Always check both error returns and result.Success
7. **Resource Cleanup**: Always call Disconnect() or Close() when done

## Thread Safety

The client is thread-safe and can be used concurrently from multiple goroutines. The connection pool is also thread-safe and designed for concurrent access.

## Dependencies

- `github.com/gorilla/websocket` - WebSocket implementation

## License

This project is licensed under the terms specified in the main KahfLane project.