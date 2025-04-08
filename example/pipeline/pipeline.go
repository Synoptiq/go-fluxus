package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"github.com/synoptiq/go-fluxus"
)

// Various data types for our pipeline stages
type RawText string
type TokenizedText []string
type FilteredTokens []string
type CountMap map[string]int
type TextStats struct {
	WordCount      int
	UniqueWords    int
	TopWords       []string
	LongestWord    string
	ShortestWord   string
	AverageLength  float64
	ProcessingTime time.Duration
}

// Stage 1: Tokenize text into words
func tokenizeText(ctx context.Context, text RawText) (TokenizedText, error) {
	startTime := time.Now()

	// Simulate processing delay
	time.Sleep(50 * time.Millisecond)

	// Check for context cancellation
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Convert to lowercase
	lowerText := strings.ToLower(string(text))

	// Remove punctuation and split into words
	re := regexp.MustCompile(`[^\w\s]`)
	cleanText := re.ReplaceAllString(lowerText, "")
	words := strings.Fields(cleanText)

	fmt.Printf("✓ Tokenized text into %d words (%.2fms)\n",
		len(words), float64(time.Since(startTime).Microseconds())/1000)

	return words, nil
}

// Stage 2: Filter out common stop words
func filterStopWords(ctx context.Context, tokens TokenizedText) (FilteredTokens, error) {
	startTime := time.Now()

	// Simulate processing delay
	time.Sleep(30 * time.Millisecond)

	// Check for context cancellation
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Common English stop words
	stopWords := map[string]bool{
		"a": true, "an": true, "the": true, "and": true, "or": true, "but": true,
		"is": true, "are": true, "was": true, "were": true, "be": true, "been": true,
		"to": true, "of": true, "in": true, "on": true, "at": true, "by": true,
		"for": true, "with": true, "about": true, "from": true, "as": true,
		"this": true, "that": true, "these": true, "those": true, "it": true,
		"i": true, "he": true, "she": true, "they": true, "we": true, "you": true,
	}

	// Filter out stop words
	filtered := make(FilteredTokens, 0, len(tokens))
	for _, word := range tokens {
		if !stopWords[word] && len(word) > 0 {
			filtered = append(filtered, word)
		}
	}

	fmt.Printf("✓ Filtered out stop words, %d words remaining (%.2fms)\n",
		len(filtered), float64(time.Since(startTime).Microseconds())/1000)

	return filtered, nil
}

// Stage 3: Count word frequencies
func countWords(ctx context.Context, tokens FilteredTokens) (CountMap, error) {
	startTime := time.Now()

	// Simulate processing delay
	time.Sleep(70 * time.Millisecond)

	// Check for context cancellation
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Count word frequencies
	wordCounts := make(CountMap)
	for _, word := range tokens {
		wordCounts[word]++
	}

	fmt.Printf("✓ Counted frequencies of %d unique words (%.2fms)\n",
		len(wordCounts), float64(time.Since(startTime).Microseconds())/1000)

	return wordCounts, nil
}

// Stage 4: Analyze text statistics
func analyzeText(ctx context.Context, counts CountMap) (TextStats, error) {
	startTime := time.Now()

	// Simulate processing delay
	time.Sleep(100 * time.Millisecond)

	// Check for context cancellation
	if ctx.Err() != nil {
		return TextStats{}, ctx.Err()
	}

	// For this example, randomly simulate an error 10% of the time
	if rand.Float32() < 0.1 {
		return TextStats{}, errors.New("random analysis error occurred")
	}

	// Calculate total words
	totalWords := 0
	for _, count := range counts {
		totalWords += count
	}

	// Find longest and shortest words
	var longestWord string
	shortestWord := "pneumonoultramicroscopicsilicovolcanoconiosis" // A very long word as default

	for word := range counts {
		if len(word) > len(longestWord) {
			longestWord = word
		}
		if len(word) < len(shortestWord) {
			shortestWord = word
		}
	}

	// Calculate average word length
	totalLength := 0
	for word, count := range counts {
		totalLength += len(word) * count
	}

	averageLength := 0.0
	if totalWords > 0 {
		averageLength = float64(totalLength) / float64(totalWords)
	}

	// Find top words (limited to top 5)
	type wordCount struct {
		word  string
		count int
	}

	wordFreqs := make([]wordCount, 0, len(counts))
	for word, count := range counts {
		wordFreqs = append(wordFreqs, wordCount{word, count})
	}

	// Sort by count (simple bubble sort for demonstration)
	for i := 0; i < len(wordFreqs); i++ {
		for j := i + 1; j < len(wordFreqs); j++ {
			if wordFreqs[j].count > wordFreqs[i].count {
				wordFreqs[i], wordFreqs[j] = wordFreqs[j], wordFreqs[i]
			}
		}
	}

	// Get top words (up to 5)
	numTop := 5
	if len(wordFreqs) < numTop {
		numTop = len(wordFreqs)
	}

	topWords := make([]string, numTop)
	for i := 0; i < numTop; i++ {
		topWords[i] = fmt.Sprintf("%s (%d)", wordFreqs[i].word, wordFreqs[i].count)
	}

	stats := TextStats{
		WordCount:      totalWords,
		UniqueWords:    len(counts),
		TopWords:       topWords,
		LongestWord:    longestWord,
		ShortestWord:   shortestWord,
		AverageLength:  averageLength,
		ProcessingTime: time.Since(startTime),
	}

	fmt.Printf("✓ Analyzed text statistics (%.2fms)\n",
		float64(stats.ProcessingTime.Microseconds())/1000)

	return stats, nil
}

// errorHandler is a custom error handler for the pipeline
func errorHandler(err error) error {
	// Log the error with a timestamp
	fmt.Printf("❌ [%s] Pipeline error: %v\n", time.Now().Format(time.RFC3339), err)

	// You could implement custom error classification and handling here
	// For example, retry on specific errors, ignore others, etc.

	// For this example, we'll just return the error
	return err
}

// displayStats prints the text statistics in a formatted way
func displayStats(text RawText, stats TextStats) {
	fmt.Println("\n📊 Text Analysis Results")
	fmt.Println("=====================")

	// Print some of the original text (truncated if too long)
	preview := string(text)
	if len(preview) > 100 {
		preview = preview[:100] + "..."
	}
	fmt.Printf("Text: %s\n\n", preview)

	fmt.Printf("Word count: %d\n", stats.WordCount)
	fmt.Printf("Unique words: %d\n", stats.UniqueWords)
	fmt.Printf("Average word length: %.2f characters\n", stats.AverageLength)
	fmt.Printf("Longest word: \"%s\" (%d characters)\n", stats.LongestWord, len(stats.LongestWord))
	fmt.Printf("Shortest word: \"%s\" (%d characters)\n", stats.ShortestWord, len(stats.ShortestWord))

	fmt.Println("\nTop words:")
	for i, word := range stats.TopWords {
		fmt.Printf("  %d. %s\n", i+1, word)
	}

	fmt.Printf("\nTotal processing time: %.2f ms\n",
		float64(stats.ProcessingTime.Microseconds())/1000)
}

// buildSimplePipeline constructs a linear pipeline using Chain
func buildSimplePipeline() *fluxus.Pipeline[RawText, TextStats] {
	// Create stages
	tokenizeStage := fluxus.StageFunc[RawText, TokenizedText](tokenizeText)
	filterStage := fluxus.StageFunc[TokenizedText, FilteredTokens](filterStopWords)
	countStage := fluxus.StageFunc[FilteredTokens, CountMap](countWords)
	analyzeStage := fluxus.StageFunc[CountMap, TextStats](analyzeText)

	// Chain stages together
	chainedStage := fluxus.Chain(
		tokenizeStage,
		fluxus.Chain(
			filterStage,
			fluxus.Chain(
				countStage,
				analyzeStage,
			),
		),
	)

	// Create a pipeline with error handling
	pipeline := fluxus.NewPipeline(chainedStage).WithErrorHandler(errorHandler)

	return pipeline
}

// buildResilientPipeline constructs a pipeline with additional resilience features
func buildResilientPipeline() *fluxus.Pipeline[RawText, TextStats] {
	// Create basic stages
	tokenizeStage := fluxus.StageFunc[RawText, TokenizedText](tokenizeText)
	filterStage := fluxus.StageFunc[TokenizedText, FilteredTokens](filterStopWords)
	countStage := fluxus.StageFunc[FilteredTokens, CountMap](countWords)
	analyzeStage := fluxus.StageFunc[CountMap, TextStats](analyzeText)

	// Add timeout to tokenize stage (tokenization should be quick)
	timedTokenizeStage := fluxus.NewTimeout(tokenizeStage, 200*time.Millisecond)

	// Add retry for the analyze stage (which has simulated random failures)
	retryAnalyzeStage := fluxus.NewRetry(analyzeStage, 3)

	// Use exponential backoff for retries
	retryAnalyzeStage.WithBackoff(func(attempt int) int {
		// 100ms, 200ms, 400ms
		return 100 * (1 << attempt)
	})

	// Chain stages together
	chainedStage := fluxus.Chain(
		timedTokenizeStage,
		fluxus.Chain(
			filterStage,
			fluxus.Chain(
				countStage,
				retryAnalyzeStage,
			),
		),
	)

	// Create a pipeline with error handling
	pipeline := fluxus.NewPipeline(chainedStage).WithErrorHandler(errorHandler)

	return pipeline
}

// runPipelineDemo runs a demonstration of a pipeline
func runPipelineDemo(name string, pipeline *fluxus.Pipeline[RawText, TextStats], text RawText) {
	fmt.Printf("\n▶️ Running %s\n", name)
	fmt.Printf("===================%s\n", strings.Repeat("=", len(name)))

	// Create a context with cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Process the text through the pipeline
	fmt.Println("Processing text through pipeline stages:")
	startTime := time.Now()

	stats, err := pipeline.Process(ctx, text)

	totalTime := time.Since(startTime)

	if err != nil {
		fmt.Printf("\n❌ Pipeline failed after %.2f ms: %v\n",
			float64(totalTime.Microseconds())/1000, err)
	} else {
		fmt.Printf("\n✅ Pipeline succeeded in %.2f ms\n",
			float64(totalTime.Microseconds())/1000)

		// Display the results
		displayStats(text, stats)
	}
}

// sampleTexts provides some examples for processing
func sampleTexts() []RawText {
	return []RawText{
		"The quick brown fox jumps over the lazy dog. This pangram contains every letter of the English alphabet.",

		"Four score and seven years ago our fathers brought forth on this continent, a new nation, " +
			"conceived in Liberty, and dedicated to the proposition that all men are created equal.",

		"It was the best of times, it was the worst of times, it was the age of wisdom, it was the age " +
			"of foolishness, it was the epoch of belief, it was the epoch of incredulity, it was the season " +
			"of Light, it was the season of Darkness, it was the spring of hope, it was the winter of despair, " +
			"we had everything before us, we had nothing before us, we were all going direct to Heaven, " +
			"we were all going direct the other way.",
	}
}

func main() {
	// Seed random for consistent results
	rand.New(rand.NewSource(time.Now().UnixNano()))

	fmt.Println("Fluxus Chain and Pipeline Composition Demonstration")
	fmt.Println("=================================================")
	fmt.Println("This example demonstrates how to build pipelines by chaining")
	fmt.Println("different stages together, with each stage transforming the data")
	fmt.Println("into a new format before passing it to the next stage.")

	// Get sample texts
	texts := sampleTexts()

	// Build different pipeline variations
	simplePipeline := buildSimplePipeline()
	resilientPipeline := buildResilientPipeline()

	// Process the first sample with simple pipeline
	fmt.Println("\n📝 Sample Text 1")
	runPipelineDemo("Simple Pipeline", simplePipeline, texts[0])

	// Process the second sample with simple pipeline too
	fmt.Println("\n📝 Sample Text 2")
	runPipelineDemo("Simple Pipeline", simplePipeline, texts[1])

	// Process the third sample with resilient pipeline
	fmt.Println("\n📝 Sample Text 3 (with Resilient Pipeline)")
	runPipelineDemo("Resilient Pipeline", resilientPipeline, texts[2])

	fmt.Println("\nDemo Complete!")
}
