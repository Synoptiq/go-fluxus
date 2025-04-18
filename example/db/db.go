package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime" // For concurrency setting
	"sync"    // For mock repo locking
	"time"

	_ "github.com/mattn/go-sqlite3" // Using testify for clearer assertions
	"github.com/synoptiq/go-fluxus"
)

// --- 1. Define the Dependency Interface (Unchanged) ---

type User struct {
	ID        int
	Email     string
	LastLogin time.Time
}

type UserRepository interface {
	GetUserByID(ctx context.Context, id int) (*User, error)
	UpdateLastLogin(ctx context.Context, id int, loginTime time.Time) error
}

// --- 2. Define the Stage Struct (Unchanged) ---

type UserProcessingStage struct {
	repo UserRepository
}

func NewUserProcessingStage(repo UserRepository) *UserProcessingStage {
	if repo == nil {
		panic("UserRepository cannot be nil")
	}
	return &UserProcessingStage{
		repo: repo,
	}
}

// --- 3. Implement the Stage Interface (Minor Logging Change) ---

func (s *UserProcessingStage) Process(ctx context.Context, userID int) (string, error) {
	// Use a logger or structured logging in real apps
	// fmt.Printf("  [Stage] Processing user ID: %d\n", userID) // Reduced verbosity for concurrent runs

	user, err := s.repo.GetUserByID(ctx, userID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("user %d not found: %w", userID, err)
		}
		return "", fmt.Errorf("failed to get user %d: %w", userID, err)
	}

	now := time.Now()
	if err := s.repo.UpdateLastLogin(ctx, userID, now); err != nil {
		// Log the error but maybe continue? Depends on requirements.
		log.Printf("Warning: failed to update last login for user %d: %v", userID, err)
	}

	summary := fmt.Sprintf("User %d (%s) login updated to %s",
		user.ID, user.Email, now.Format(time.RFC3339))

	// fmt.Printf("  [Stage] Finished processing user ID: %d\n", userID) // Reduced verbosity
	return summary, nil
}

var _ fluxus.Stage[int, string] = (*UserProcessingStage)(nil)

// --- 4a. Concrete Dependency Implementation (SQLite - Unchanged) ---

type SQLiteUserRepository struct {
	db *sql.DB
}

func NewSQLiteUserRepository(db *sql.DB) *SQLiteUserRepository {
	if db == nil {
		panic("sql.DB cannot be nil")
	}
	return &SQLiteUserRepository{db: db}
}

func (r *SQLiteUserRepository) GetUserByID(ctx context.Context, id int) (*User, error) {
	query := "SELECT id, email, last_login FROM users WHERE id = ?"
	row := r.db.QueryRowContext(ctx, query, id)

	var user User
	var lastLoginStr string

	err := row.Scan(&user.ID, &user.Email, &lastLoginStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("query user %d failed: %w", id, err)
	}

	user.LastLogin, err = time.Parse(time.RFC3339, lastLoginStr)
	if err != nil {
		// Handle potential empty string if last_login is NULL
		if lastLoginStr == "" {
			// Assign a zero time or handle as appropriate
			user.LastLogin = time.Time{}
		} else {
			return nil, fmt.Errorf("parsing last_login '%s' for user %d failed: %w", lastLoginStr, id, err)
		}
	}

	return &user, nil
}

func (r *SQLiteUserRepository) UpdateLastLogin(ctx context.Context, id int, loginTime time.Time) error {
	query := "UPDATE users SET last_login = ? WHERE id = ?"
	_, err := r.db.ExecContext(ctx, query, loginTime.Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("update last_login for user %d failed: %w", id, err)
	}
	// fmt.Printf("  [SQLiteDB] Updated last login for user %d\n", id) // Reduced verbosity
	return nil
}

// --- 4b. Concrete Dependency Implementation (Mock - Added Mutex for Concurrency) ---

type MockUserRepository struct {
	mu    sync.RWMutex // Added mutex for safe concurrent access in tests
	users map[int]*User

	GetUserByIDFunc     func(ctx context.Context, id int) (*User, error)
	UpdateLastLoginFunc func(ctx context.Context, id int, loginTime time.Time) error
}

func NewMockUserRepository() *MockUserRepository {
	m := &MockUserRepository{
		users: map[int]*User{
			1: {ID: 1, Email: "alice@example.com", LastLogin: time.Now().Add(-24 * time.Hour)},
			2: {ID: 2, Email: "bob@example.com", LastLogin: time.Now().Add(-48 * time.Hour)},
			4: {ID: 4, Email: "charlie@example.com", LastLogin: time.Now().Add(-72 * time.Hour)}, // Added more users
			5: {ID: 5, Email: "dave@example.com", LastLogin: time.Now().Add(-96 * time.Hour)},
		},
	}

	// Default implementations using the mutex
	m.GetUserByIDFunc = func(_ context.Context, id int) (*User, error) {
		m.mu.RLock()
		defer m.mu.RUnlock()
		user, exists := m.users[id]
		if !exists {
			return nil, sql.ErrNoRows
		}
		userCopy := *user // Return a copy
		return &userCopy, nil
	}

	m.UpdateLastLoginFunc = func(_ context.Context, id int, loginTime time.Time) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		user, exists := m.users[id]
		if !exists {
			return fmt.Errorf("user %d not found for update", id)
		}
		user.LastLogin = loginTime
		// fmt.Printf("  [MockDB] Updated last login for user %d to %s\n", id, loginTime.Format(time.RFC3339))
		return nil
	}

	return m
}

func (m *MockUserRepository) GetUserByID(ctx context.Context, id int) (*User, error) {
	return m.GetUserByIDFunc(ctx, id)
}

func (m *MockUserRepository) UpdateLastLogin(ctx context.Context, id int, loginTime time.Time) error {
	return m.UpdateLastLoginFunc(ctx, id, loginTime)
}

// --- Database Setup Helper (Unchanged) ---

const dbFile = "./fluxus_di_example.db"

func setupDatabase(ctx context.Context) (*sql.DB, error) {
	_ = os.Remove(dbFile)
	db, err := sql.Open("sqlite3", dbFile+"?_journal_mode=WAL&_busy_timeout=5000") // Added WAL mode and busy timeout for better concurrency
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	createTableSQL := `
	CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL UNIQUE,
		last_login TEXT
	);`
	if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create users table: %w", err)
	}

	insertSQL := "INSERT INTO users (id, email, last_login) VALUES (?, ?, ?)"
	usersToInsert := []User{
		{ID: 1, Email: "alice@example.com", LastLogin: time.Now().Add(-24 * time.Hour)},
		{ID: 2, Email: "bob@example.com", LastLogin: time.Now().Add(-48 * time.Hour)},
		{ID: 4, Email: "charlie@example.com", LastLogin: time.Now().Add(-72 * time.Hour)},
		{ID: 5, Email: "dave@example.com", LastLogin: time.Now().Add(-96 * time.Hour)},
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		tx.Rollback()
		db.Close()
		return nil, fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, user := range usersToInsert {
		_, err := stmt.ExecContext(ctx, user.ID, user.Email, user.LastLogin.Format(time.RFC3339))
		if err != nil {
			tx.Rollback()
			db.Close()
			return nil, fmt.Errorf("failed to insert user %d: %w", user.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Println("✅ SQLite database initialized.")
	return db, nil
}

// --- 5. Example Usage (Using fluxus.Map) ---

func main() {
	fmt.Println("🚀 Fluxus Dependency Injection Example (with SQLite & Map Stage)")
	fmt.Println("==============================================================")

	ctx := context.Background()

	// --- Setup Real Database ---
	db, err := setupDatabase(ctx)
	if err != nil {
		log.Fatalf("Database setup failed: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbFile)

	// --- Create the Real Repository ---
	userRepo := NewSQLiteUserRepository(db)

	// --- Create the Stage to be Mapped ---
	// Create the single stage instance that will process each user ID.
	// It's instantiated once with its dependency.
	processStage := NewUserProcessingStage(userRepo)

	// --- Create the Map Stage ---
	// Map takes a slice of inputs ([]int) and applies the processStage
	// to each element concurrently.
	// Limit concurrency to avoid overwhelming the DB connection pool or CPU.
	concurrency := runtime.NumCPU() * 2 // Example concurrency limit
	mapStage := fluxus.NewMap(processStage).
		WithConcurrency(concurrency).
		WithCollectErrors(true) // Collect all errors instead of failing on the first one

	// --- Create and Run Pipeline ---
	// The pipeline now takes []int as input and produces []string as output.
	pipeline := fluxus.NewPipeline(mapStage)

	// Process a slice of user IDs
	userIDs := []int{1, 3, 2, 99, 4, 5} // Includes non-existent IDs

	fmt.Printf("\nProcessing %d user IDs concurrently (limit %d)...\n", len(userIDs), concurrency)
	startTime := time.Now()
	results, err := pipeline.Process(ctx, userIDs)
	duration := time.Since(startTime)
	fmt.Printf("Processing finished in %v\n", duration)

	if err != nil {
		// WithCollectErrors, 'err' will be a fluxus.MultiError
		fmt.Printf("❌ Errors occurred during processing:\n")
		if merr, ok := err.(*fluxus.MultiError); ok {
			for i, e := range merr.Errors {
				if e != nil { // Check if there was an error for this specific index
					fmt.Printf("  - Input index %d (ID %d): %v\n", i, userIDs[i], e)
				}
			}
		} else {
			fmt.Printf("  - Unexpected error type: %v\n", err) // Should not happen with WithCollectErrors
		}
	}

	fmt.Println("\n✅ Successful Results:")
	if len(results) > 0 {
		for i, result := range results {
			// Check if the corresponding error was nil before printing the result
			isError := false
			if merr, ok := err.(*fluxus.MultiError); ok && i < len(merr.Errors) && merr.Errors[i] != nil {
				isError = true
			}
			if !isError {
				fmt.Printf("  - Input index %d (ID %d): %s\n", i, userIDs[i], result)
			}
		}
	} else if err == nil {
		fmt.Println("  (No results - perhaps all inputs failed?)")
	}

	// --- Verify DB State (Optional) ---
	fmt.Println("\nVerifying SQLite DB state:")
	// ... (verification logic remains the same) ...
	rows, err := db.QueryContext(ctx, "SELECT id, email, last_login FROM users ORDER BY id")
	if err != nil {
		log.Printf("Warning: Failed to query DB for verification: %v", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var user User
			var lastLoginStr string
			if err := rows.Scan(&user.ID, &user.Email, &lastLoginStr); err != nil {
				log.Printf("Warning: Failed to scan row: %v", err)
				continue
			}
			user.LastLogin, _ = time.Parse(time.RFC3339, lastLoginStr)
			fmt.Printf("  User %d (%s) - Last Login: %s\n", user.ID, user.Email, user.LastLogin.Format(time.RFC3339))
		}
	}
}
