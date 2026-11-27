package graph

import pb "github.com/swytchdb/engine/cluster/proto"

// Codec maps materialized scalar collection elements to the standard data
// representation. Encode creates a fresh replacement; Decode borrows immutable
// input and must copy any returned mutable buffers.
type Codec[T any] struct {
	Encode func(T) *pb.DataEffect
	Decode func(*pb.DataEffect) T
}

// MapType describes a standalone or composed map using a scalar element codec.
func MapType[T any](codec Codec[T]) Type[*MapField[T]] {
	return Type[*MapField[T]]{New: func() *MapField[T] { return NewMapField(Composite{}, codec.Encode, codec.Decode) }}
}

// ListType describes a standalone or composed list using a scalar element codec.
func ListType[T any](codec Codec[T]) Type[*ListField[T]] {
	return Type[*ListField[T]]{New: func() *ListField[T] { return NewListField(Composite{}, codec.Encode, codec.Decode) }}
}

// NewMap creates an independently usable map. It can later be composed without
// losing its identity or pending edits.
func NewMap[T any](codec Codec[T]) *MapField[T] { return MapType(codec).New() }

// NewList creates an independently usable list with pending insertions.
func NewList[T any](codec Codec[T], values ...T) *ListField[T] {
	return ListType(codec).New().Append(values...)
}

// StringSetType describes an empty string set.
var StringSetType = Type[*StringSetField]{New: func() *StringSetField { return NewStringSetField(Composite{}) }}

// NewStringSet creates a standalone string set with pending memberships.
func NewStringSet(values ...string) *StringSetField { return StringSetType.New().Add(values...) }

// Runtime implements Value for maps, whether standalone or composed.
func (m *MapField[T]) Runtime() *Composite { return &m.scope }

// Runtime implements Value for lists, whether standalone or composed.
func (l *ListField[T]) Runtime() *Composite { return &l.scope }

// Runtime implements Value for sets, whether standalone or composed.
func (s *StringSetField) Runtime() *Composite { return &s.scope }

// StringCodec is the standard scalar collection representation for string.
var StringCodec = Codec[string]{
	Encode: func(value string) *pb.DataEffect {
		return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_Raw{Raw: []byte(value)}}
	},
	Decode: func(data *pb.DataEffect) string { return string(data.Decompress()) },
}

// BytesCodec is the standard scalar collection representation for []byte.
var BytesCodec = Codec[[]byte]{
	Encode: func(value []byte) *pb.DataEffect {
		return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_Raw{Raw: append([]byte(nil), value...)}}
	},
	Decode: func(data *pb.DataEffect) []byte { return append([]byte(nil), data.Decompress()...) },
}

// Int64Codec is the standard scalar collection representation for int64.
var Int64Codec = Codec[int64]{
	Encode: func(value int64) *pb.DataEffect {
		return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: value}}
	},
	Decode: func(data *pb.DataEffect) int64 { return data.GetIntVal() },
}

// Float64Codec is the standard scalar collection representation for float64.
var Float64Codec = Codec[float64]{
	Encode: func(value float64) *pb.DataEffect {
		return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_FloatVal{FloatVal: value}}
	},
	Decode: func(data *pb.DataEffect) float64 { return data.GetFloatVal() },
}

// Uint32Codec is the standard scalar collection representation for uint32.
var Uint32Codec = Codec[uint32]{
	Encode: func(value uint32) *pb.DataEffect {
		return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: int64(value)}}
	},
	Decode: func(data *pb.DataEffect) uint32 { return uint32(data.GetIntVal()) },
}

// Uint8Codec is the standard scalar collection representation for uint8.
var Uint8Codec = Codec[uint8]{
	Encode: func(value uint8) *pb.DataEffect {
		return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: int64(value)}}
	},
	Decode: func(data *pb.DataEffect) uint8 { return uint8(data.GetIntVal()) },
}

// BoolCodec is the standard scalar collection representation for bool.
var BoolCodec = Codec[bool]{
	Encode: func(value bool) *pb.DataEffect {
		var encoded int64
		if value {
			encoded = 1
		}

		return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: encoded}}
	},
	Decode: func(data *pb.DataEffect) bool { return data.GetIntVal() != 0 },
}
