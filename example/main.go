package main

import (
	"fmt"
	"log"
	"time"

	kahflane "github.com/KahfLand/kahflane_dbaas/clients/go"
)

func main() {
	// --- Configuration ---
	// https://
	serverURL := "ws://localhost:8080" // test-khf-7084
	rootUser := "root"
	rootPassword := ""
	mainDatabase := "main"
	testDatabase := "testdb"
	newUser := "testuser"
	newUserPassword := "password123"

	// --- 1. Initial Setup & Login as Root ---
	log.Println("--- Step 1: Connecting as root user ---")
	client := kahflane.NewClient(kahflane.ConnectionConfig{URL: serverURL})

	// Set up event handlers to see what's happening
	client.SetEventHandlers(map[string]interface{}{
		"connected": func(info kahflane.ConnectionInfo) {
			log.Printf("✅ Connected to server version %s (Connection ID: %s)", info.ServerVersion, info.ConnectionID)
		},
		"disconnected": func(reason string) {
			log.Printf("🔌 Disconnected: %s", reason)
		},
		"error": func(err string, messageID *string) {
			if messageID != nil {
				log.Printf("❌ Server Error (for message %s): %s", *messageID, err)
			} else {
				log.Printf("❌ Server Error: %s", err)
			}
		},
		"backup_progress": func(progress *kahflane.BackupProgress) {
			log.Printf("⏳ Backup Progress: [%s] %d%% - %s", progress.Stage, int(progress.Percentage), progress.Message)
		},
		"restore_progress": func(progress *kahflane.RestoreProgress) {
			log.Printf("⏳ Restore Progress: [%s] %d%% - %s", progress.Stage, int(progress.Percentage), progress.Message)
		},
	})

	_, err := client.Login(rootUser, rootPassword, mainDatabase)
	if err != nil {
		log.Fatalf("Failed to log in as root: %v", err)
	}

	if _, err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect as root: %v", err)
	}
	defer client.Disconnect()

	// --- 2. Create a New Database ---
	log.Println("\n--- Step 2: Creating a new database ---")
	_, err = client.Query(fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDatabase), &kahflane.QueryOptions{Timeout: 30 * time.Minute})
	if err != nil {
		log.Fatalf("Failed to drop existing test database: %v", err)
	}
	log.Println("Dropped existing test database (if any).")

	res, err := client.Query(fmt.Sprintf("CREATE DATABASE %s", testDatabase), &kahflane.QueryOptions{Timeout: 15 * time.Minute})
	if err != nil || !res.Success {
		log.Fatalf("Failed to create database: %v, Result: %+v", err, res)
	}
	log.Printf("✅ Database '%s' created successfully.", testDatabase)

	// --- 3. CRUD Operations as Root ---
	log.Println("\n--- Step 3: Performing CRUD operations as root ---")
	// Create Table
	_, err = client.Execute(
		"CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR)",
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	log.Println("✅ Table 'users' created.")

	// Insert Data
	_, err = client.Execute(
		"INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
		[]interface{}{1, "Alice", "alice@example.com"},
	)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	log.Println("✅ Inserted initial data for Alice.")

	// Read Data
	res, err = client.Execute("SELECT * FROM users", nil)
	if err != nil {
		log.Fatalf("Failed to read data: %v", err)
	}
	log.Printf("✅ Read data after insert: %+v", res.Data)

	// Update Data
	_, err = client.Execute(
		"UPDATE users SET email = ? WHERE id = ?",
		[]interface{}{"alice.new@example.com", 1},
	)
	if err != nil {
		log.Fatalf("Failed to update data: %v", err)
	}
	log.Println("✅ Updated Alice's email.")

	// Read Data Again
	res, err = client.Execute("SELECT * FROM users", nil)
	if err != nil {
		log.Fatalf("Failed to read data after update: %v", err)
	}
	log.Printf("✅ Read data after update: %+v", res.Data)

	// --- 4. Backup and Restore ---
	log.Println("\n--- Step 4: Performing Backup and Restore ---")
	backupRes, err := client.StartBackup(kahflane.BackupOptions{Description: "Test Backup"})
	if err != nil {
		log.Fatalf("Failed to start backup: %v", err)
	}
	if !backupRes.Success {
		log.Fatalf("Backup operation failed: %s", backupRes.Error)
	}
	log.Printf("✅ Backup started successfully. Filename: %s", backupRes.BackupInfo.Filename)
	time.Sleep(5 * time.Second) // Give backup some time to complete

	restoreRes, err := client.StartRestore(kahflane.RestoreOptions{BackupFilename: backupRes.BackupInfo.Filename})
	if err != nil {
		log.Fatalf("Failed to start restore: %v", err)
	}
	if !restoreRes.Success {
		log.Fatalf("Restore operation failed: %s", restoreRes.Error)
	}
	log.Println("✅ Restore started successfully.")
	time.Sleep(5 * time.Second) // Give restore some time to complete

	// --- 5. Create a New User ---
	log.Println("\n--- Step 5: Creating a new user ---")
	createUserRes, err := client.CreateUser(&kahflane.CreateUserRequest{Username: newUser, Password: newUserPassword})
	if err != nil {
		log.Fatalf("Failed to create user: %v", err)
	}
	if !createUserRes.Success {
		log.Fatalf("User creation failed: %s", createUserRes.Error)
	}
	log.Printf("✅ User '%s' created.", newUser)

	// Grant privileges to the new user on the test database
	privileges := []string{"select", "insert", "update", "delete"}
	for _, privilege := range privileges {
		grantRes, err := client.GrantPrivilege(&kahflane.GrantPrivilegeRequest{
			Username:  newUser,
			Database:  testDatabase,
			Privilege: privilege,
		})
		if err != nil {
			log.Fatalf("Failed to grant privileges: %v", err)
		}
		if !grantRes.Success {
			log.Fatalf("Grant privilege operation failed: %s", grantRes.Error)
		}
	}
	log.Printf("✅ Granted ALL privileges on '%s' to '%s'.", testDatabase, newUser)

	// --- 6. Connect and Perform CRUD as New User ---
	log.Println("\n--- Step 6: Connecting and performing CRUD as new user ---")
	newUserClient := kahflane.NewClient(kahflane.ConnectionConfig{URL: serverURL})
	_, err = newUserClient.Login(newUser, newUserPassword, testDatabase)
	if err != nil {
		log.Fatalf("Failed to log in as new user: %v", err)
	}
	if _, err := newUserClient.Connect(); err != nil {
		log.Fatalf("Failed to connect as new user: %v", err)
	}
	defer newUserClient.Disconnect()
	log.Printf("✅ Connected successfully as '%s'.", newUser)

	// Insert as new user
	_, err = newUserClient.Execute(
		fmt.Sprintf("INSERT INTO %s.users (id, name, email) VALUES (?, ?, ?)", testDatabase),
		[]interface{}{2, "Bob", "bob@example.com"},
	)
	if err != nil {
		log.Fatalf("New user failed to insert data: %v", err)
	}
	log.Println("✅ New user inserted data for Bob.")

	// Read as new user
	res, err = newUserClient.Execute(fmt.Sprintf("SELECT * FROM %s.users ORDER BY id", testDatabase), nil)
	if err != nil {
		log.Fatalf("New user failed to read data: %v", err)
	}
	log.Printf("✅ New user read data: %+v", res.Data)

	// Delete as new user
	_, err = newUserClient.Execute(
		fmt.Sprintf("DELETE FROM %s.users WHERE id = ?", testDatabase),
		[]interface{}{1},
	)
	if err != nil {
		log.Fatalf("New user failed to delete data: %v", err)
	}
	log.Println("✅ New user deleted Alice's record.")

	// Final Read
	res, err = newUserClient.Execute(fmt.Sprintf("SELECT * FROM %s.users", testDatabase), nil)
	if err != nil {
		log.Fatalf("New user failed to perform final read: %v", err)
	}
	log.Printf("✅ Final data state: %+v", res.Data)

	log.Println("\n--- ✅ Example script completed successfully! ---")
}
