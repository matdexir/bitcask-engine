package engine_test

import (
	"bitcask/engine"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func setupTestDirForT(t *testing.T) string {
	dir := filepath.Join(os.TempDir(), "bitcask_test")
	os.RemoveAll(dir)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

func setupTestDirForB(b *testing.B) string {
	dir := filepath.Join(os.TempDir(), "bitcask_test_")
	os.RemoveAll(dir)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

func cleanupTestDir(dir string) {
	os.RemoveAll(dir)
}

func generateKey(i int) string {
	return fmt.Sprintf("key_%010d", i)
}

// generateValue generates a value of a specific size
func generateValue(size int) string {
	return strings.Repeat("x", size)
}

func TestPutAndGet(t *testing.T) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	dir := setupTestDirForT(t)

	engine, err := engine.NewBistcaskEngine(dir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	defer engine.Close()

	key := "foo"
	value := "bar"

	err = engine.Put(key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	result, err := engine.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result != value {
		t.Errorf("Expected value '%s', got '%s'", value, result)
	}
}

func TestOverwrite(t *testing.T) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	dir := setupTestDirForT(t)

	engine, err := engine.NewBistcaskEngine(dir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	defer engine.Close()

	key := "foo"

	firstValue := "bar"
	secondValue := "barbar"

	err = engine.Put(key, firstValue)
	if err != nil {
		t.Fatalf("Put first value failed: %v", err)
	}

	err = engine.Put(key, secondValue)
	if err != nil {
		t.Fatalf("Put second value failed: %v", err)
	}

	result, err := engine.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result != secondValue {
		t.Errorf("Expected value '%s', got '%s'", secondValue, result)
	}
}

func TestDelete(t *testing.T) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	dir := setupTestDirForT(t)

	engine, err := engine.NewBistcaskEngine(dir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	defer engine.Close()

	key := "foo"

	value := "bar"

	err = engine.Put(key, value)
	if err != nil {
		t.Fatalf("Put first value failed: %v", err)
	}

	engine.Delete(key)

	_, err = engine.Get(key)
	if err == nil {
		t.Errorf("Expected value '%s' to be deleted", value)
	}
}

func TestPersistence(t *testing.T) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	tmpDir := t.TempDir()

	engine1, err := engine.NewBistcaskEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	err = engine1.Put("hello", "world")
	if err != nil {
		t.Fatalf("Put value failed: %v", err)
	}
	engine1.Close()

	// Now reopen
	engine2, err := engine.NewBistcaskEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	err = engine2.BuildIndex()
	if err != nil {
		t.Fatalf("BuildIndex Failed: '%v'", err)
	}

	val, err := engine2.Get("hello")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "world" {
		t.Fatalf("the value '%s' is not the same as expected '%s'", val, "world")
	}
}

func TestRollOver(t *testing.T) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	tmpDir := t.TempDir()
	engine, err := engine.NewBistcaskEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	engine.MaxFileSize = 200 // force very small file size

	for i := range 10 {
		key := fmt.Sprintf("key%d", i)
		val := strings.Repeat("x", 20)
		err = engine.Put(key, val)
		time.Sleep(500 * time.Millisecond)
		if err != nil {
			t.Fatalf("Put value failed: %v", err)
		}
	}
	files, _ := os.ReadDir(tmpDir)

	dataFileCount := 0
	for _, f := range files {
		log.Printf("test file: %s", f)
		if strings.HasSuffix(f.Name(), ".data") {
			dataFileCount++
		}
	}

	if dataFileCount < 2 {
		t.Fatalf("Roll over failed since we only have '%d' files", dataFileCount)
	}

}

// BenchmarkGetSequential measures sequential Get performance on pre-populated data
func BenchmarkGetSequential(b *testing.B) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	dir := setupTestDirForB(b)
	defer cleanupTestDir(dir)

	engine, err := engine.NewBistcaskEngine(dir)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	engine.MaxFileSize = 10 * 1024 * 1024

	// Pre-populate the database with b.N keys/values
	valueSize := 100 // bytes
	value := generateValue(valueSize)
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("get_seq_key_%d", i)
		err := engine.Put(key, value)
		if err != nil {
			b.Fatalf("Setup Put error: %v", err)
		}
		keys[i] = key
	}
	// Important: Ensure the index is built if your system requires it for reading old files
	// If the current implementation only reads from ActiveFile until rollover, this might not be strictly necessary for small N.
	// However, for realistic testing, you'd want to test reading from merged files too.
	// For now, let's assume all data is in the active file or that BuildIndex is called implicitly by NewBitcaskEngine if it finds existing files.
	// Your current NewBitcaskEngine does not call BuildIndex, so we'd need to call it manually if testing reads across multiple files.
	// For simple sequential GET, if all Puts happen in one active file, it's fine.
	// If MaxFileSize is hit during setup, BuildIndex would be needed to read from older files.

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := engine.Get(keys[i]) // Get the key that was just put
		if err != nil {
			b.Fatalf("Get error: %v", err)
		}
	}
}

// BenchmarkGetConcurrent measures concurrent Get performance
func BenchmarkGetConcurrent(b *testing.B) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	dir := setupTestDirForB(b)
	defer cleanupTestDir(dir)

	engine, err := engine.NewBistcaskEngine(dir)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	engine.MaxFileSize = 10 * 1024 * 1024

	// Pre-populate with a reasonable number of keys
	// It's usually better to populate with a fixed large N, then run b.N Get operations
	// to avoid "setup time" skewing results for small b.N.
	numPrepopulate := 100000 // Populate with 100k keys for realistic read testing
	valueSize := 100
	value := generateValue(valueSize)
	prepopulatedKeys := make([]string, numPrepopulate)

	for i := range numPrepopulate {
		key := fmt.Sprintf("get_con_key_%d", i)
		err := engine.Put(key, value)
		if err != nil {
			b.Fatalf("Setup Put error: %v", err)
		}
		prepopulatedKeys[i] = key
	}
	// After populating, ensure index is built if files rolled over
	// If you want to test reads across multiple files, call engine.BuildIndex() here.
	// For this benchmark, we're assuming the NewBistcaskEngine might rebuild or that all reads are from active file.
	// If MaxFileSize is small, BuildIndex is critical here.
	if err := engine.BuildIndex(); err != nil {
		b.Fatalf("Failed to build index after prepopulation: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Randomly pick a key from the prepopulated set
			keyIndex := rand.Intn(numPrepopulate)
			key := prepopulatedKeys[keyIndex]
			_, err := engine.Get(key)
			if err != nil {
				b.Errorf("Get error for key %s: %v", key, err)
			}
		}
	})
}

// BenchmarkDeleteSequential measures sequential Delete performance
func BenchmarkDeleteSequential(b *testing.B) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	dir := setupTestDirForB(b)
	defer cleanupTestDir(dir)

	engine, err := engine.NewBistcaskEngine(dir)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	engine.MaxFileSize = 10 * 1024 * 1024

	// Pre-populate
	valueSize := 100
	value := generateValue(valueSize)
	keysToDelete := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("del_seq_key_%d", i)
		err := engine.Put(key, value)
		if err != nil {
			b.Fatalf("Setup Put error: %v", err)
		}
		keysToDelete[i] = key
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := engine.Delete(keysToDelete[i])
		if err != nil {
			b.Fatalf("Delete error: %v", err)
		}
	}
}

// BenchmarkDeleteConcurrent measures concurrent Delete performance
func BenchmarkDeleteConcurrent(b *testing.B) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	dir := setupTestDirForB(b)
	defer cleanupTestDir(dir)

	engine, err := engine.NewBistcaskEngine(dir)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	engine.MaxFileSize = 10 * 1024 * 1024

	// Pre-populate with more keys than b.N to allow random deletions
	numPrepopulate := 100000 // Delete from a larger set
	valueSize := 100
	value := generateValue(valueSize)
	prepopulatedKeys := make([]string, numPrepopulate)

	for i := 0; i < numPrepopulate; i++ {
		key := fmt.Sprintf("del_con_key_%d", i)
		err := engine.Put(key, value)
		if err != nil {
			b.Fatalf("Setup Put error: %v", err)
		}
		prepopulatedKeys[i] = key
	}
	// After populating, ensure index is built if files rolled over
	if err := engine.BuildIndex(); err != nil {
		b.Fatalf("Failed to build index after prepopulation: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keyIndex := rand.Intn(numPrepopulate)
			key := prepopulatedKeys[keyIndex]
			err := engine.Delete(key)
			// Deleting the same key concurrently might lead to "key not found" errors
			// if another goroutine already deleted it. For strict throughput,
			// it's better to ensure unique deletions if possible, or handle expected errors.
			// For now, we'll just report.
			if err != nil && !strings.Contains(err.Error(), "key not found") {
				b.Errorf("Delete error for key %s: %v", key, err)
			}
		}
	})
}

// BenchmarkPutSequential measures sequential Put performance
func BenchmarkPutSequential(b *testing.B) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	dir := setupTestDirForB(b)
	defer cleanupTestDir(dir)

	engine, err := engine.NewBistcaskEngine(dir)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	engine.MaxFileSize = 10 * 1024 * 1024 // 10MB for rollover testing

	keyPrefix := "put_seq_key_"
	valueSize := 100 // bytes
	value := generateValue(valueSize)

	b.ResetTimer()   // Start timing from here
	b.ReportAllocs() // Report memory allocations

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)
		err := engine.Put(key, value)
		if err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}
}

// BenchmarkPutConcurrent measures concurrent Put performance
func BenchmarkPutConcurrent(b *testing.B) {
	originalOutput := log.Writer()

	// Redirect log output to discard
	log.SetOutput(io.Discard)

	// Restore original output after the test
	defer log.SetOutput(originalOutput)
	dir := setupTestDirForB(b)
	defer cleanupTestDir(dir)

	engine, err := engine.NewBistcaskEngine(dir)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	engine.MaxFileSize = 10 * 1024 * 1024 // 10MB for rollover testing

	valueSize := 100 // bytes
	value := generateValue(valueSize)

	// Pre-generate keys to avoid generating them inside the timed loop
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("put_con_key_%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		// workerID := 0 // Not strictly unique, but indicates parallel execution
		for pb.Next() {
			// Use a different key for each parallel operation to avoid contention on the same key
			// Note: This needs careful consideration if you want to test updates vs. new puts
			// For simple throughput, unique keys are better.
			keyIndex := rand.Intn(b.N) // Randomly pick a key from pre-generated set
			key := keys[keyIndex]

			err := engine.Put(key, value)
			if err != nil {
				b.Errorf("Put error: %v", err) // Use b.Errorf in RunParallel
			}
		}
	})
}
