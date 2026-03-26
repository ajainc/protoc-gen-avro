package avro

import (
  "fmt"
  "github.com/ajainc/protoc-gen-avro/input"
  "slices"
  "strings"
)

type TypeRepo struct {
  Types map[string]NamedType
  seenTypes map[string]bool // go "set"
  NamespaceMap map[string]string
  // FileNamespaceMap stores per-file namespace overrides from proto file-level options.
  // Keys are proto package names (exact match), values are Avro namespaces.
  // Takes precedence over NamespaceMap.
  FileNamespaceMap map[string]string
  CollapseFields []string
  RemoveEnumPrefixes bool
  PreserveNonStringMaps bool
}

func NewTypeRepo(params input.Params) *TypeRepo {
  return &TypeRepo{
    Types: make(map[string]NamedType),
    NamespaceMap: params.NamespaceMap,
    FileNamespaceMap: make(map[string]string),
    CollapseFields: params.CollapseFields,
    RemoveEnumPrefixes: params.RemoveEnumPrefixes,
    PreserveNonStringMaps: params.PreserveNonStringMaps,
  }
}

func (r *TypeRepo) AddType(t NamedType) {
  fullName := FullName(t)
  r.Types[fullName] = t
}

func (r *TypeRepo) GetTypeByBareName(name string) Type {
  for _, t := range r.Types {
    if t.GetName() == name {
      return t
    }
  }
  return nil
}

func (r *TypeRepo) SeenType(t NamedType) {
  r.seenTypes[FullName(t)] = true
}

func (r *TypeRepo) GetType(name string) (Type, error) {
  if r.seenTypes[name] {
    if r.Types[name] != nil {
      if slices.Contains(r.CollapseFields, r.Types[name].GetName()) {
        return r.Types[name].(Record).Fields[0].Type, nil
      }
    }
    return Bare(r.MappedNamespace(name[1:])), nil
  }
  t, ok := r.Types[name]
  if !ok {
    return nil, fmt.Errorf("type %s not found", name)
  }
  r.SeenType(t)
  return t, nil
}

func (r *TypeRepo) Start() {
  r.seenTypes = map[string]bool{}
}

func (r *TypeRepo) LogTypes() {
	var keys []string
	for k := range r.Types {
		keys = append(keys, k)
	}
	LogObj(keys)
}

func (r *TypeRepo) MappedNamespace(namespace string) string {
  // File-level option takes precedence (exact prefix match on package).
  for pkg, ns := range r.FileNamespaceMap {
    if namespace == pkg {
      return ns
    }
    if strings.HasPrefix(namespace, pkg+".") {
      return ns + namespace[len(pkg):]
    }
  }
  out := namespace
  for k, v := range r.NamespaceMap {
    out = strings.Replace(out, k, v, -1)
  }
  return out
}
