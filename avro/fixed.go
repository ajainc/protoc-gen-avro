package avro

import (
	"fmt"

	"github.com/iancoleman/orderedmap"
)

// Fixed represents an Avro fixed type with optional logicalType metadata.
// Used for decimal and other logicalType annotations.
type Fixed struct {
	Name        string
	Namespace   string
	Size        int
	LogicalType string
	Precision   int
	Scale       int
}

func (f Fixed) GetName() string {
	return f.Name
}

func (f Fixed) GetNamespace() string {
	return f.Namespace
}

func (f Fixed) ToJSON(types *TypeRepo) (any, error) {
	// Avro named types must be defined only once per schema;
	// subsequent references use the name string alone.
	if types != nil {
		key := FullName(f)
		if types.seenTypes != nil && types.seenTypes[key] {
			return f.Name, nil
		}
		types.SeenType(f)
	}

	jsonMap := orderedmap.New()
	jsonMap.Set("type", "fixed")
	jsonMap.Set("name", f.Name)
	jsonMap.Set("size", f.Size)
	if f.LogicalType != "" {
		jsonMap.Set("logicalType", f.LogicalType)
	}
	if f.LogicalType == "decimal" {
		jsonMap.Set("precision", f.Precision)
		jsonMap.Set("scale", f.Scale)
	}
	return jsonMap, nil
}

// FixedName generates a deterministic name from logicalType parameters.
// e.g. precision=38, scale=9, size=16 → "Decimal38_9_16"
func FixedName(logicalType string, precision, scale, size int) string {
	if logicalType == "decimal" {
		return fmt.Sprintf("Decimal%d_%d_%d", precision, scale, size)
	}
	return logicalType
}
