package graph

import (
	pb "github.com/swytchdb/engine/cluster/proto"
)

// String is a standalone Swytch string value. It retains its identity and
// pending operations when composed. Emission consumes all sharing values.
// It is not safe for concurrent mutation. Snapshots seed reads, not effects.
type String struct{ Composite }

// StringType constructs an empty String for loading or generic composition.
var StringType = Type[*String]{New: func() *String { return &String{} }}

// NewString creates a value with a pending replacement. Byte inputs are copied.
func NewString(value string) *String { return (&String{}).Set(value) }

// Runtime implements Value without changing materialized reads.
func (s *String) Runtime() *Composite {
	s.scalar = true

	return &s.Composite
}

// Value returns the current local value, including pending mutations.
func (s *String) Value() string { return string(s.current().GetScalar().Decompress()) }

// Set queues a replacement in place and returns this same value.
func (s *String) Set(value string) *String {
	s.Runtime().apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_Raw{Raw: []byte(value)}}}}})

	return s
}

// Bytes is a standalone Swytch []byte value. It retains its identity and
// pending operations when composed. Emission consumes all sharing values.
// It is not safe for concurrent mutation. Snapshots seed reads, not effects.
type Bytes struct{ Composite }

// BytesType constructs an empty Bytes for loading or generic composition.
var BytesType = Type[*Bytes]{New: func() *Bytes { return &Bytes{} }}

// NewBytes creates a value with a pending replacement. Byte inputs are copied.
func NewBytes(value []byte) *Bytes { return (&Bytes{}).Set(value) }

// Runtime implements Value without changing materialized reads.
func (s *Bytes) Runtime() *Composite {
	s.scalar = true

	return &s.Composite
}

// Value returns the current local value, including pending mutations.
func (s *Bytes) Value() []byte { return append([]byte(nil), s.current().GetScalar().Decompress()...) }

// Set queues a replacement in place and returns this same value.
func (s *Bytes) Set(value []byte) *Bytes {
	s.Runtime().apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_Raw{Raw: append([]byte(nil), value...)}}}}})

	return s
}

// Int64 is a standalone Swytch int64 value. It retains its identity and
// pending operations when composed. Emission consumes all sharing values.
// It is not safe for concurrent mutation. Snapshots seed reads, not effects.
type Int64 struct{ Composite }

// Int64Type constructs an empty Int64 for loading or generic composition.
var Int64Type = Type[*Int64]{New: func() *Int64 { return &Int64{} }}

// NewInt64 creates a value with a pending replacement. Byte inputs are copied.
func NewInt64(value int64) *Int64 { return (&Int64{}).Set(value) }

// Runtime implements Value without changing materialized reads.
func (s *Int64) Runtime() *Composite {
	s.scalar = true

	return &s.Composite
}

// Value returns the current local value, including pending mutations.
func (s *Int64) Value() int64 { return s.current().GetScalar().GetIntVal() }

// Set queues a replacement in place and returns this same value.
func (s *Int64) Set(value int64) *Int64 {
	s.Runtime().apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: value}}}}})

	return s
}

// Float64 is a standalone Swytch float64 value. It retains its identity and
// pending operations when composed. Emission consumes all sharing values.
// It is not safe for concurrent mutation. Snapshots seed reads, not effects.
type Float64 struct{ Composite }

// Float64Type constructs an empty Float64 for loading or generic composition.
var Float64Type = Type[*Float64]{New: func() *Float64 { return &Float64{} }}

// NewFloat64 creates a value with a pending replacement. Byte inputs are copied.
func NewFloat64(value float64) *Float64 { return (&Float64{}).Set(value) }

// Runtime implements Value without changing materialized reads.
func (s *Float64) Runtime() *Composite {
	s.scalar = true

	return &s.Composite
}

// Value returns the current local value, including pending mutations.
func (s *Float64) Value() float64 { return s.current().GetScalar().GetFloatVal() }

// Set queues a replacement in place and returns this same value.
func (s *Float64) Set(value float64) *Float64 {
	s.Runtime().apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_FloatVal{FloatVal: value}}}}})

	return s
}

// Uint32 is a standalone Swytch uint32 value. It retains its identity and
// pending operations when composed. Emission consumes all sharing values.
// It is not safe for concurrent mutation. Snapshots seed reads, not effects.
type Uint32 struct{ Composite }

// Uint32Type constructs an empty Uint32 for loading or generic composition.
var Uint32Type = Type[*Uint32]{New: func() *Uint32 { return &Uint32{} }}

// NewUint32 creates a value with a pending replacement. Byte inputs are copied.
func NewUint32(value uint32) *Uint32 { return (&Uint32{}).Set(value) }

// Runtime implements Value without changing materialized reads.
func (s *Uint32) Runtime() *Composite {
	s.scalar = true

	return &s.Composite
}

// Value returns the current local value, including pending mutations.
func (s *Uint32) Value() uint32 { return uint32(s.current().GetScalar().GetIntVal()) }

// Set queues a replacement in place and returns this same value.
func (s *Uint32) Set(value uint32) *Uint32 {
	s.Runtime().apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: int64(value)}}}}})

	return s
}

// Uint8 is a standalone Swytch uint8 value. It retains its identity and
// pending operations when composed. Emission consumes all sharing values.
// It is not safe for concurrent mutation. Snapshots seed reads, not effects.
type Uint8 struct{ Composite }

// Uint8Type constructs an empty Uint8 for loading or generic composition.
var Uint8Type = Type[*Uint8]{New: func() *Uint8 { return &Uint8{} }}

// NewUint8 creates a value with a pending replacement. Byte inputs are copied.
func NewUint8(value uint8) *Uint8 { return (&Uint8{}).Set(value) }

// Runtime implements Value without changing materialized reads.
func (s *Uint8) Runtime() *Composite {
	s.scalar = true

	return &s.Composite
}

// Value returns the current local value, including pending mutations.
func (s *Uint8) Value() uint8 { return uint8(s.current().GetScalar().GetIntVal()) }

// Set queues a replacement in place and returns this same value.
func (s *Uint8) Set(value uint8) *Uint8 {
	s.Runtime().apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: int64(value)}}}}})

	return s
}

// Bool is a standalone Swytch bool value. It retains its identity and
// pending operations when composed. Emission consumes all sharing values.
// It is not safe for concurrent mutation. Snapshots seed reads, not effects.
type Bool struct{ Composite }

// BoolType constructs an empty Bool for loading or generic composition.
var BoolType = Type[*Bool]{New: func() *Bool { return &Bool{} }}

// NewBool creates a value with a pending replacement. Byte inputs are copied.
func NewBool(value bool) *Bool { return (&Bool{}).Set(value) }

// Runtime implements Value without changing materialized reads.
func (s *Bool) Runtime() *Composite {
	s.scalar = true

	return &s.Composite
}

// Value returns the current local value, including pending mutations.
func (s *Bool) Value() bool { return s.current().GetScalar().GetIntVal() != 0 }

// Set queues a replacement in place and returns this same value.
func (s *Bool) Set(value bool) *Bool {
	var encoded int64
	if value {
		encoded = 1
	}

	s.Runtime().apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: encoded}}}}})

	return s
}

// Add queues an additive delta, retaining the snapshot and previous operations.
func (s *Int64) Add(value int64) *Int64 {
	s.Runtime().apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT, Value: &pb.DataEffect_IntVal{IntVal: value}}}}})

	return s
}

// Add queues an additive delta, retaining the snapshot and previous operations.
func (s *Float64) Add(value float64) *Float64 {
	s.Runtime().apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_FLOAT, Value: &pb.DataEffect_FloatVal{FloatVal: value}}}}})

	return s
}

// Max queues a maximum candidate using the engine's MAX_INT rule.
func (s *Int64) Max(value int64) *Int64 {
	s.Runtime().apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_MAX_INT, Value: &pb.DataEffect_IntVal{IntVal: value}}}}})

	return s
}

// Append queues a suffix using the existing scalar string effect semantics.
func (s *String) Append(value string) *String {
	s.Runtime().apply((&StringEffect{}).Append(value).getEffects())

	return s
}

// Prepend queues a prefix using the existing scalar string effect semantics.
func (s *String) Prepend(value string) *String {
	s.Runtime().apply((&StringEffect{}).Prepend(value).getEffects())

	return s
}

// Insert queues text before a byte offset. Invalid offsets panic when reduced.
func (s *String) Insert(value string, at int) *String {
	s.Runtime().apply((&StringEffect{}).Insert(value, at).getEffects())

	return s
}
