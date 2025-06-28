package engine

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"log"
	"time"
)

type FileEntry struct {
	Crc         uint32
	Tstamp      int64
	Ksz         uint32
	ValueSz     uint32
	Key         string
	Value       string
	IsTombstone bool
}

func (fe *FileEntry) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(fe)
	if err != nil {
		log.Printf("Unable to encode FileEntry into binary: %v", err)
		return nil, fmt.Errorf("unable to encode FileEntry into binary: %w", err)
	}
	return buf.Bytes(), nil
}

func NewFileEntry(key, value string, isTombstone bool) (*FileEntry, error) {

	tstamp := time.Now().Unix()

	hasher := crc32.NewIEEE()
	hasher.Write([]byte(key))
	hasher.Write([]byte(value))

	return &FileEntry{
		Crc:         hasher.Sum32(),
		Tstamp:      tstamp,
		Ksz:         uint32(len(key)),
		ValueSz:     uint32(len(value)),
		Key:         key,
		Value:       value,
		IsTombstone: isTombstone,
	}, nil

}

func DeserializeFileEntry(buffer []byte) (FileEntry, error) {
	var fe FileEntry
	dec := gob.NewDecoder(bytes.NewReader(buffer))
	if err := dec.Decode(&fe); err != nil {
		log.Printf("Unable to decode FileEntry from buffer: %v", err)
		return FileEntry{}, fmt.Errorf("unable to decode FileEntry: %w", err)
	}
	return fe, nil
}
