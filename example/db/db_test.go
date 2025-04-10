package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/synoptiq/go-fluxus"
)

func TestUserProcessingStage(t *testing.T) {
	mockRepo := NewMockUserRepository()
	stage := NewUserProcessingStage(mockRepo)
	ctx := context.Background()

	t.Run("UserExists", func(t *testing.T) {
		userID := 1
		result, err := stage.Process(ctx, userID)
		require.NoError(t, err)
		assert.Contains(t, result, "User 1 (alice@example.com)")

		updatedUser, _ := mockRepo.GetUserByID(ctx, userID)
		assert.WithinDuration(t, time.Now(), updatedUser.LastLogin, 1*time.Second)
	})

	t.Run("UserNotFound", func(t *testing.T) {
		userID := 99
		_, err := stage.Process(ctx, userID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "user 99 not found")
		assert.ErrorIs(t, err, sql.ErrNoRows) // Check underlying error type if needed
	})

	t.Run("GetUserError", func(t *testing.T) {
		originalGetUserFunc := mockRepo.GetUserByIDFunc
		simulatedErr := errors.New("simulated DB connection error")
		mockRepo.GetUserByIDFunc = func(ctx context.Context, id int) (*User, error) {
			return nil, simulatedErr
		}
		defer func() { mockRepo.GetUserByIDFunc = originalGetUserFunc }()

		userID := 1
		_, err := stage.Process(ctx, userID)
		require.Error(t, err)
		assert.ErrorIs(t, err, simulatedErr)
		assert.Contains(t, err.Error(), "failed to get user 1")
	})

	t.Run("UpdateUserError", func(t *testing.T) {
		originalUpdateFunc := mockRepo.UpdateLastLoginFunc
		simulatedErr := errors.New("simulated update error")
		mockRepo.UpdateLastLoginFunc = func(ctx context.Context, id int, loginTime time.Time) error {
			return simulatedErr
		}
		defer func() { mockRepo.UpdateLastLoginFunc = originalUpdateFunc }()

		userID := 1
		// We still expect success from Process, as the error is only logged
		result, err := stage.Process(ctx, userID)
		require.NoError(t, err) // Error is logged, not returned by Process
		assert.Contains(t, result, "User 1")
		// Note: Testing log output requires more setup (capturing log output)
	})
}

// --- 7. Unit Testing Example (Pipeline with Map Stage) ---

func TestUserProcessingPipelineWithMap(t *testing.T) {
	mockRepo := NewMockUserRepository()
	processStage := NewUserProcessingStage(mockRepo) // Stage with mock repo
	mapStage := fluxus.NewMap(processStage).
		WithConcurrency(2).     // Use some concurrency for the test
		WithCollectErrors(true) // Important for testing multiple outcomes

	ctx := context.Background()

	t.Run("MixedSuccessAndFailure", func(t *testing.T) {
		userIDs := []int{1, 99, 2, 100}                                                               // Good, Not Found, Good, Not Found
		expectedResults := []string{"User 1 (alice@example.com)", "", "User 2 (bob@example.com)", ""} // Expected results (empty for errors)

		// We call directly mapStage.Process instead of pipeline.Process because we want
		// both the errors and the partial results. If we used pipeline.Process,
		// it would return only the errors and nil for results.
		// This is a common pattern when testing stages that can fail independently.
		results, err := mapStage.Process(ctx, userIDs)

		// Check for MultiError
		require.Error(t, err, "Expected an error because some inputs failed")
		merr, ok := err.(*fluxus.MultiError)
		require.True(t, ok, "Expected error to be a MultiError")

		// --- FIX: Assert the number of actual errors ---
		require.Len(t, merr.Errors, 2, "MultiError should contain 2 actual errors")

		// --- FIX: Check results slice (which has length matching input) ---
		require.Len(t, results, len(userIDs))
		assert.Contains(t, results[0], expectedResults[0], "Result for ID 1 mismatch")
		assert.Equal(t, "", results[1], "Result for ID 99 should be empty string on error")
		assert.Contains(t, results[2], expectedResults[2], "Result for ID 2 mismatch")
		assert.Equal(t, "", results[3], "Result for ID 100 should be empty string on error")

		// --- FIX: Check the content of the collected errors ---
		foundErr99 := false
		foundErr100 := false
		for _, itemErr := range merr.Errors {
			// Check if it's a MapItemError
			var mapItemErr *fluxus.MapItemError
			if errors.As(itemErr, &mapItemErr) {
				if mapItemErr.ItemIndex == 1 { // Error for input index 1 (value 99)
					assert.ErrorIs(t, mapItemErr.OriginalError, sql.ErrNoRows)
					assert.Contains(t, mapItemErr.Error(), "user 99 not found")
					foundErr99 = true
				} else if mapItemErr.ItemIndex == 3 { // Error for input index 3 (value 100)
					assert.ErrorIs(t, mapItemErr.OriginalError, sql.ErrNoRows)
					assert.Contains(t, mapItemErr.Error(), "user 100 not found")
					foundErr100 = true
				}
			}
		}
		assert.True(t, foundErr99, "Did not find expected error for user 99")
		assert.True(t, foundErr100, "Did not find expected error for user 100")
	})

	t.Run("SimulatedGetErrorDuringMap", func(t *testing.T) {
		userIDs := []int{1, 4} // Process two users

		// Simulate GetUserByID error only for the second user (ID 4)
		originalGetUserFunc := mockRepo.GetUserByIDFunc
		simulatedErr := errors.New("simulated get error for ID 4")
		mockRepo.GetUserByIDFunc = func(ctx context.Context, id int) (*User, error) {
			if id == 4 {
				return nil, simulatedErr // Fail only for ID 4
			}
			// Use original for others (make sure it's correctly captured)
			// Need to ensure mockRepo is accessible here or capture originalGetUserFunc correctly
			// Assuming originalGetUserFunc correctly points to the initial mock function
			return originalGetUserFunc(ctx, id)
		}
		defer func() { mockRepo.GetUserByIDFunc = originalGetUserFunc }() // Restore

		results, err := mapStage.Process(ctx, userIDs)

		require.Error(t, err)
		merr, ok := err.(*fluxus.MultiError)
		require.True(t, ok)

		// --- FIX: Assert the number of actual errors ---
		require.Len(t, merr.Errors, 1, "MultiError should contain 1 actual error")

		// --- FIX: Check results slice ---
		require.Len(t, results, 2)
		assert.Contains(t, results[0], "User 1", "Result for ID 1 should be present")
		assert.Equal(t, "", results[1], "Result for ID 4 should be empty string")

		// --- FIX: Check the single error in merr.Errors ---
		singleErr := merr.Errors[0]

		// Check if it's a MapItemError and for the correct index
		var mapItemErr *fluxus.MapItemError
		require.ErrorAs(t, singleErr, &mapItemErr, "Error should be a MapItemError")
		assert.Equal(t, 1, mapItemErr.ItemIndex, "Error should be for item index 1") // Index 1 corresponds to input user ID 4
		assert.ErrorIs(t, mapItemErr.OriginalError, simulatedErr, "Original error should be the simulated one")
	})

	t.Run("AllSuccess", func(t *testing.T) {
		userIDs := []int{1, 2, 4, 5} // All exist in mock repo

		results, err := mapStage.Process(ctx, userIDs)

		require.NoError(t, err, "Expected no error when all inputs succeed")
		require.Len(t, results, len(userIDs))
		assert.Contains(t, results[0], "User 1")
		assert.Contains(t, results[1], "User 2")
		assert.Contains(t, results[2], "User 4")
		assert.Contains(t, results[3], "User 5")
	})
}
