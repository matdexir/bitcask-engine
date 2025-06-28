package engine

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Bitcask interface {
	Get(key string) (string, error)
	Put(key string, value string) error
	Delete(key string) error
	BuildIndex() error
	Merge() error
}

type BitcaskEngine struct {
	Keydir      map[string]*KeyDir
	ActiveFile  *os.File
	ActiveDir   string
	mu          sync.RWMutex
	MaxFileSize int64
}

func NewBistcaskEngine(directory string) (*BitcaskEngine, error) {
	_, err := os.Stat(directory)
	if os.IsNotExist(err) {
		permissions := os.FileMode(0755)
		err = os.MkdirAll(directory, permissions)
		if err != nil {
			// Using log.Fatalf here because failing to create the directory is a critical setup error.
			log.Fatalf("Error creating directory '%s': %v", directory, err)
			return nil, err // This line will not be reached due to Fatalln
		}
	} else if err != nil {
		// Using log.Fatalf here for similar reasons, unable to check directory is critical.
		log.Fatalf("Error checking directory '%s': '%v'", directory, err)
		return nil, err // This line will not be reached due to Fatalln
	}

	filename := fmt.Sprintf("%d.data", time.Now().Unix())
	activeFilePath := filepath.Join(directory, filename)

	// Original fmt.Printf commented out, log.Printf can be used if needed for debug
	// log.Printf("chosen file name is %s", activeFilePath)

	newActiveFile, err := os.OpenFile(activeFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// Fatal error if we can't create the active file.
		log.Fatalf("Error creating new file '%s': '%v'", activeFilePath, err)
		return nil, err // This line will not be reached due to Fatalln
	}

	return &BitcaskEngine{
		Keydir:      make(map[string]*KeyDir),
		ActiveFile:  newActiveFile,
		ActiveDir:   directory,
		MaxFileSize: 1 * 1024 * 1024, // 1MB
	}, nil
}

func (be *BitcaskEngine) Close() error {
	be.mu.Lock()
	defer be.mu.Unlock()

	if be.ActiveFile == nil {
		return nil
	}
	err := be.ActiveFile.Close()
	be.ActiveFile = nil
	if err != nil {
		log.Printf("Error closing active file: %v", err)
	}
	return err
}

func (be *BitcaskEngine) Get(key string) (string, error) {
	be.mu.Lock()
	defer be.mu.Unlock()

	record, ok := be.Keydir[key]
	if !ok {
		log.Printf("Unable to find key '%s' in keydir", key)
		return "", fmt.Errorf("key not found error")
	}

	entry, err := be.fetchFromDisk(record)
	if err != nil {
		log.Printf("Unable to fetch record '%v' from disk: '%v'", record, err)
		return "", fmt.Errorf("unable to fetch record from disk error: %w", err)
	}
	if entry.IsTombstone {
		log.Printf("Attempted to retrieve deleted key '%s'", key)
		return "", fmt.Errorf("key '%s' has been deleted", key)
	}

	return entry.Value, nil
}

func (be *BitcaskEngine) fetchFromDisk(record *KeyDir) (*FileEntry, error) {

	file, err := os.Open(record.FileID)
	if err != nil {
		log.Printf("Unable to open file '%s': '%v'", record.FileID, err)
		return nil, fmt.Errorf("unable to open file '%s': %w", record.FileID, err)
	}
	defer file.Close()
	payloadStartOffset := record.ValuePos + 8
	payloadLength := int64(record.ValueSz) - 8

	if payloadLength < 0 { // Sanity check
		log.Printf("Invalid payload length calculated for record %v", record)
		return nil, fmt.Errorf("invalid payload length calculated for record %v", record)
	}

	buf := make([]byte, uint64(payloadLength))
	_, err = file.ReadAt(buf, payloadStartOffset)
	if err != nil {
		log.Printf("Unable to read the buffer at offset '%d' with size '%d': '%v'", record.ValuePos, record.ValueSz, err)
		return nil, fmt.Errorf("unable to read buffer at offset '%d' with size '%d': %w", record.ValuePos, record.ValueSz, err)
	}

	fileEntry, err := DeserializeFileEntry(buf)
	if err != nil {
		log.Printf("Failed to deserialize file entry: %v", err)
		return nil, fmt.Errorf("failed to deserialize file entry: %w", err)
	}
	return &fileEntry, nil
}

func (be *BitcaskEngine) Put(key, value string) error {
	be.mu.Lock()
	defer be.mu.Unlock()

	fileEntry, err := NewFileEntry(key, value, false)
	if err != nil {
		log.Printf("Failed to create new file entry for key '%s': %v", key, err)
		return fmt.Errorf("failed to create file entry: %w", err)
	}

	keydirEntry, err := be.putFileEntry(fileEntry)
	if err != nil {
		log.Printf("Unable to insert key '%s' and value '%s' into disk: %v", key, value, err)
		return fmt.Errorf("unable to insert key-value pair into disk: %w", err)
	}
	be.Keydir[key] = keydirEntry
	return nil
}

func (be *BitcaskEngine) Delete(key string) error {
	be.mu.Lock()
	defer be.mu.Unlock()

	if _, ok := be.Keydir[key]; !ok {
		log.Printf("Attempted to delete non-existent key '%s'", key)
		// NOTE: I'm unsure if this is an error or not
		return fmt.Errorf("key '%s' not found for deletion", key)
	}

	tombstoneEntry, err := NewFileEntry(key, "", true)
	if err != nil {
		log.Printf("Failed to create tombstone entry for key '%s': '%v'", key, err)
		return fmt.Errorf("failed to create tombstone entry for key '%s': %w", key, err)

	}
	_, err = be.putFileEntry(tombstoneEntry)
	if err != nil {
		log.Printf("Unable to put tombstone value into disk for key '%s': %v", key, err)
		return fmt.Errorf("unable to put tombstone value into disk: %w", err)
	}
	delete(be.Keydir, key)
	log.Printf("Key '%s' successfully marked as deleted and removed from keydir", key)
	return nil
}

func (be *BitcaskEngine) putFileEntry(fileEntry *FileEntry) (*KeyDir, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(fileEntry)
	if err != nil {
		log.Printf("Failed to encode file entry: %v", err)
		return nil, fmt.Errorf("failed to encode file entry: %w", err)
	}

	serializedFileEntryBytes := buf.Bytes()
	payloadLen := uint64(len(serializedFileEntryBytes))
	lenBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBuf, payloadLen)

	totalLen := int64(8 + payloadLen)

	// Check if file rollover is needed
	fileInfo, err := be.ActiveFile.Stat()
	if err != nil {
		log.Printf("Cannot stat active file: %v", err)
		return nil, fmt.Errorf("cannot stat active file: %w", err)
	}

	if fileInfo.Size()+totalLen > be.MaxFileSize {
		log.Printf("Active file size %d bytes, rolling over to new file. Current entry size: %d bytes. MaxFileSize is %d", fileInfo.Size(), totalLen, be.MaxFileSize)
		err := be.rollOverActiveFile()
		if err != nil {
			log.Printf("Failed to roll over active file: %v", err)
			return nil, fmt.Errorf("failed to roll over active file: %w", err)
		}
	}

	offset, err := be.ActiveFile.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Printf("Unable to get current file offset: '%v'", err)
		return nil, fmt.Errorf("unable to get current file offset: %w", err)
	}

	_, err = be.ActiveFile.Write(lenBuf)
	if err != nil {
		log.Printf("Unable to write the correct buffer length for the file entry: %v", err)
		return nil, fmt.Errorf("unable to write length prefix: %w", err)
	}

	nbytes, err := be.ActiveFile.Write(serializedFileEntryBytes)
	if err != nil {
		log.Printf("Unable to write to file: '%v'", err)
		return nil, fmt.Errorf("unable to write file entry payload: %w", err)
	}

	if uint64(nbytes) != payloadLen {
		log.Printf("Mismatch between payload length '%d' and bytes written '%d'", payloadLen, nbytes)
		return nil, fmt.Errorf("write size mismatch: expected %d bytes, wrote %d", payloadLen, nbytes)
	}

	keydirEntry := &KeyDir{
		FileID:   be.ActiveFile.Name(),
		ValueSz:  uint64(8 + nbytes),
		ValuePos: offset,
		Tstamp:   fileEntry.Tstamp,
	}

	return keydirEntry, nil
}

func (be *BitcaskEngine) rollOverActiveFile() error {
	if be.ActiveFile != nil {
		log.Printf("Closing active file: %s", be.ActiveFile.Name())
		err := be.ActiveFile.Close()
		if err != nil {
			log.Printf("Error closing old active file '%s': %v", be.ActiveFile.Name(), err)
			// Decide if this is a critical error or just log and proceed. For now, we'll return.
			return fmt.Errorf("error closing old active file: %w", err)
		}
	}

	filename := fmt.Sprintf("%d.data", time.Now().Unix())
	activeFilePath := filepath.Join(be.ActiveDir, filename)

	// Original fmt.Printf commented out
	log.Printf("Chosen new active file name is %s", activeFilePath)

	newActiveFile, err := os.OpenFile(activeFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Unable to roll over to new active file '%s': '%v'", activeFilePath, err)
		return fmt.Errorf("unable to open new active file '%s': %w", activeFilePath, err)
	}
	be.ActiveFile = newActiveFile
	log.Printf("Successfully rolled over to new active file: %s", newActiveFile.Name())
	return nil
}

func (be *BitcaskEngine) BuildIndex() error {
	directoryEntries, err := os.ReadDir(be.ActiveDir)
	if err != nil {
		log.Printf("Unable to get all the subdirectories of '%s': %v", be.ActiveDir, err)
		return fmt.Errorf("unable to read directory '%s': %w", be.ActiveDir, err)
	}

	sort.Slice(directoryEntries, func(i, j int) bool {
		fileI := strings.TrimSuffix(directoryEntries[i].Name(), ".data")
		fileJ := strings.TrimSuffix(directoryEntries[j].Name(), ".data")

		tsI, errI := strconv.ParseInt(fileI, 10, 64)
		tsJ, errJ := strconv.ParseInt(fileJ, 10, 64)

		if errI != nil || errJ != nil {
			log.Printf("Warning: Non-timestamp filename found, falling back to lexicographical sort: %s or %s", directoryEntries[i].Name(), directoryEntries[j].Name())
			return directoryEntries[i].Name() < directoryEntries[j].Name()
		}
		return tsI < tsJ
	})

	for _, fileInfo := range directoryEntries {
		if fileInfo.IsDir() || !strings.HasSuffix(fileInfo.Name(), ".data") {
			continue
		}

		filePath := filepath.Join(be.ActiveDir, fileInfo.Name())
		err = be.processOldFile(filePath)
		if err != nil {
			log.Printf("Failed processing of file '%s': %v", fileInfo.Name(), err)
			return fmt.Errorf("failed processing file '%s': %w", fileInfo.Name(), err)
		}
	}

	log.Println("Index built successfully.")
	return nil
}

func (be *BitcaskEngine) processOldFile(filePath string) error {
	log.Printf("Processing file '%s'", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Unable to open file '%s': %v", filePath, err)
		return fmt.Errorf("unable to open file '%s': %w", filePath, err)
	}
	defer file.Close()

	currentOffset := int64(0)

	for {
		lenBuf := make([]byte, 8)
		n, err := io.ReadFull(file, lenBuf)
		if err == io.EOF {
			break // End of file, no more records
		}
		if err != nil {
			log.Printf("Error reading length prefix from '%s' at offset %d: %v", filePath, currentOffset, err)
			return fmt.Errorf("error reading length prefix from '%s' at offset %d: %w", filePath, currentOffset, err)
		}
		if n != 8 {
			log.Printf("Short read for length prefix in '%s' at offset %d, expected 8 bytes, got %d", filePath, currentOffset, n)
			return fmt.Errorf("short read for length prefix in '%s' at offset %d, expected 8 bytes, got %d", filePath, currentOffset, n)
		}

		payloadLen := binary.BigEndian.Uint64(lenBuf)
		recordStartOffset := currentOffset

		payloadOffset := currentOffset + 8

		payloadBuf := make([]byte, payloadLen)
		n, err = io.ReadFull(file, payloadBuf)
		if err != nil {
			log.Printf("Error reading payload from '%s' at offset %d (payload length %d): %v", filePath, payloadOffset, payloadLen, err)
			return fmt.Errorf("error reading payload from '%s' at offset %d (payload length %d): %w", filePath, payloadOffset, payloadLen, err)
		}
		if uint64(n) != payloadLen {
			log.Printf("Short read for payload in '%s' at offset %d, expected %d bytes, got %d", filePath, payloadOffset, payloadLen, n)
			return fmt.Errorf("short read for payload in '%s' at offset %d, expected %d bytes, got %d", filePath, payloadOffset, payloadLen, n)
		}

		fe, err := DeserializeFileEntry(payloadBuf)
		if err != nil {
			log.Printf("Error deserializing FileEntry from '%s' at offset %d: %v", filePath, payloadOffset, err)
			return fmt.Errorf("error deserializing FileEntry from '%s' at offset %d: %w", filePath, payloadOffset, err)
		}

		recordTotalSize := uint64(8) + payloadLen

		existingKeyDirEntry, ok := be.Keydir[fe.Key]

		if fe.IsTombstone {
			if !ok || fe.Tstamp >= existingKeyDirEntry.Tstamp {
				delete(be.Keydir, fe.Key)
				log.Printf("Deleted key '%s' from keydir during index build (tombstone from %s)", fe.Key, filePath)
			} else {
				log.Printf("Skipping older tombstone for key '%s' from %s", fe.Key, filePath)
			}
		} else {
			if !ok || fe.Tstamp >= existingKeyDirEntry.Tstamp {
				be.Keydir[fe.Key] = &KeyDir{
					FileID:   filePath,
					ValueSz:  recordTotalSize,
					ValuePos: recordStartOffset, // Offset of the start of this complete record (including length prefix)
					Tstamp:   fe.Tstamp,
				}
				log.Printf("Updated keydir for '%s' from file '%s'", fe.Key, filePath)
			} else {
				log.Printf("Skipping older entry for key '%s' from %s (current timestamp %d, existing timestamp %d)", fe.Key, filePath, fe.Tstamp, existingKeyDirEntry.Tstamp)
			}
		}

		currentOffset += int64(recordTotalSize)
	}
	return nil
}
