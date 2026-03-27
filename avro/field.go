package avro

import (
	"fmt"

	"github.com/ajainc/protoc-gen-avro/avropb"
	"github.com/iancoleman/orderedmap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

type noDefaultType struct{}

var noDefault = noDefaultType{}

type Field struct {
	Name    string
	Type    Type
	Default any
}

func (t Field) ToJSON(types *TypeRepo) (any, error) {
	typeJson, err := t.Type.ToJSON(types)
	if err != nil {
		return nil, fmt.Errorf("error parsing field type %s %w", t.Name, err)
	}
	jsonMap := orderedmap.New()
	jsonMap.Set("name", t.Name)
	jsonMap.Set("type", typeJson)
	var defaultValue any
	if t.Default != "" {
		defaultValue = t.Default
	} else if isFixedType(t.Type) {
		// Fixed types (e.g. decimal) have no sensible default — omit the default key.
		defaultValue = noDefault
	} else {
		defaultValue = DefaultValue(typeJson)
	}
	// Avro can't actually handle defaults for records
	if defaultValue != noDefault {
		jsonMap.Set("default", defaultValue)
	}

	return jsonMap, nil
}

// isFixedType returns true if the underlying type is Fixed (not wrapped in Union/Array).
func isFixedType(t Type) bool {
	_, ok := t.(Fixed)
	return ok
}

func FieldFromProto(fdp *descriptorpb.FieldDescriptorProto) Field {
	name := fdp.GetName()
	if opts := getAvroFieldOptions(fdp); opts != nil && opts.FieldName != "" {
		name = opts.FieldName
	}
	return Field{
		Name:    name,
		Type:    FieldTypeFromProto(fdp),
		Default: fdp.GetDefaultValue(),
	}
}

func FieldTypeFromProto(fdp *descriptorpb.FieldDescriptorProto) Type {
	basicType := BasicFieldTypeFromProto(fdp)
	if fdp.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		return Array{Items: basicType}
	} else if fdp.GetProto3Optional() {
		return Union{Types: []Type{Bare("null"), basicType}}
	} else {
		return basicType
	}
}

func BasicFieldTypeFromProto(fdp *descriptorpb.FieldDescriptorProto) Type {
	if opts := getAvroFieldOptions(fdp); opts != nil && opts.LogicalType == "decimal" {
		if err := validateDecimalOptions(fdp.GetName(), opts); err != nil {
			LogMsg("ERROR: %v", err)
			return Bare("bytes")
		}
		name := opts.Name
		if name == "" {
			name = FixedName(opts.LogicalType, int(opts.Precision), int(opts.Scale))
		}
		return Fixed{
			Name:        name,
			Size:        int(opts.FixedSize),
			LogicalType: opts.LogicalType,
			Precision:   int(opts.Precision),
			Scale:       int(opts.Scale),
		}
	}

	switch fdp.GetType() {
	case descriptorpb.FieldDescriptorProto_TYPE_FLOAT:
		return Bare("float")
	case descriptorpb.FieldDescriptorProto_TYPE_DOUBLE:
		return Bare("double")
	case descriptorpb.FieldDescriptorProto_TYPE_INT64:
		return Bare("long")
	case descriptorpb.FieldDescriptorProto_TYPE_UINT64:
		return Bare("long")
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED64:
		return Bare("long")
	case descriptorpb.FieldDescriptorProto_TYPE_SINT64:
		return Bare("long")
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED64:
		return Bare("long")
	case descriptorpb.FieldDescriptorProto_TYPE_INT32:
		return Bare("int")
	case descriptorpb.FieldDescriptorProto_TYPE_UINT32:
		return Bare("int")
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED32:
		return Bare("int")
	case descriptorpb.FieldDescriptorProto_TYPE_SINT32:
		return Bare("int")
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED32:
		return Bare("int")
	case descriptorpb.FieldDescriptorProto_TYPE_BOOL:
		return Bare("boolean")
	case descriptorpb.FieldDescriptorProto_TYPE_STRING:
		return Bare("string")
	case descriptorpb.FieldDescriptorProto_TYPE_MESSAGE:
		return Ref(fdp.GetTypeName())
	case descriptorpb.FieldDescriptorProto_TYPE_BYTES:
		return Bare("bytes")
	case descriptorpb.FieldDescriptorProto_TYPE_ENUM:
		return Ref(fdp.GetTypeName())
	}
	return Bare(fdp.GetName())
}

func validateDecimalOptions(fieldName string, opts *avropb.AvroFieldOptions) error {
	if opts.Precision < 1 {
		return fmt.Errorf("field %s: decimal precision must be >= 1, got %d", fieldName, opts.Precision)
	}
	if opts.FixedSize < 1 {
		return fmt.Errorf("field %s: decimal fixed_size must be >= 1, got %d", fieldName, opts.FixedSize)
	}
	if opts.Scale < 0 {
		return fmt.Errorf("field %s: decimal scale must be >= 0, got %d", fieldName, opts.Scale)
	}
	if opts.Scale > opts.Precision {
		return fmt.Errorf("field %s: decimal scale (%d) must be <= precision (%d)", fieldName, opts.Scale, opts.Precision)
	}
	return nil
}

func getAvroFieldOptions(fdp *descriptorpb.FieldDescriptorProto) *avropb.AvroFieldOptions {
	if fdp.GetOptions() == nil {
		return nil
	}
	ext := proto.GetExtension(fdp.GetOptions(), avropb.E_Avro)
	if ext == nil {
		return nil
	}
	opts, ok := ext.(*avropb.AvroFieldOptions)
	if !ok {
		return nil
	}
	return opts
}

func DefaultValue(t any) any {
	switch t {
	case "null":
		return nil
	case "boolean":
		return false
	case "int":
		return 0
	case "long":
		return 0
	case "float":
		return 0.0
	case "double":
		return 0.0
	case "map":
		return map[string]any{}
	case "record":
		return noDefault
	case "array":
		return []any{}
	}

	switch typedT := t.(type) {
	case []any:
		return DefaultValue(typedT[0])
	case *orderedmap.OrderedMap:
		val, _ := typedT.Get("type")
		if val == "enum" {
			defaultVal, _ := typedT.Get("default")
			return defaultVal
		}
		// Fixed types have no sensible default — omit the default key.
		// In unions (["null", fixed]), the []any branch handles the null default.
		if val == "fixed" {
			return noDefault
		}
		return DefaultValue(val)
	}

	return ""
}
