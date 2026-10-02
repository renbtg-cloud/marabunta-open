// Marabunta - Licensed under the MIT License.
// Package types defines shared types used across the PostgreSQL plugin.
package types

// ColumnDef describes a column in a row description.
type ColumnDef struct {
	Name     string
	OID      int32 // type OID
	TypeSize int16 // type size (-1 for variable)
	TypeMod  int32 // type modifier
	Format   int16 // 0 = text, 1 = binary
}
