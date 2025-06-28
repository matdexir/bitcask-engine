package engine

import (
	"testing"
)

func TestFileEntry_SerializeDeserialize(t *testing.T) {
	original, err := NewFileEntry("foo", "bar", false)
	if err != nil {
		t.Fatalf("Failed to create FileEntry: %v", err)
	}

	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	deserialized, err := DeserializeFileEntry(data)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	if original.Crc != deserialized.Crc {
		t.Errorf("CRC mismatch: got %v, want %v", deserialized.Crc, original.Crc)
	}
	if original.Tstamp != deserialized.Tstamp {
		t.Errorf("Timestamp mismatch: got %v, want %v", deserialized.Tstamp, original.Tstamp)
	}
	if original.Ksz != deserialized.Ksz {
		t.Errorf("Key size mismatch: got %v, want %v", deserialized.Ksz, original.Ksz)
	}
	if original.ValueSz != deserialized.ValueSz {
		t.Errorf("Value size mismatch: got %v, want %v", deserialized.ValueSz, original.ValueSz)
	}
	if original.Key != deserialized.Key {
		t.Errorf("Key mismatch: got %v, want %v", deserialized.Key, original.Key)
	}
	if original.Value != deserialized.Value {
		t.Errorf("Value mismatch: got %v, want %v", deserialized.Value, original.Value)
	}
}
