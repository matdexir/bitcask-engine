package engine_test

import (
	"bitcask/engine"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func setupTestDir(t *testing.T) string {
	dir := filepath.Join(os.TempDir(), "bitcask_test")
	os.RemoveAll(dir)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

func TestPutAndGet(t *testing.T) {
	dir := setupTestDir(t)

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
	dir := setupTestDir(t)

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
	dir := setupTestDir(t)

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
