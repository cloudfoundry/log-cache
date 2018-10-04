// Code generated by protoc-gen-go. DO NOT EDIT.
// source: promql.proto

package logcache_v1

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	context "golang.org/x/net/context"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type PromQL struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PromQL) Reset()         { *m = PromQL{} }
func (m *PromQL) String() string { return proto.CompactTextString(m) }
func (*PromQL) ProtoMessage()    {}
func (*PromQL) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0}
}

func (m *PromQL) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL.Unmarshal(m, b)
}
func (m *PromQL) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL.Marshal(b, m, deterministic)
}
func (m *PromQL) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL.Merge(m, src)
}
func (m *PromQL) XXX_Size() int {
	return xxx_messageInfo_PromQL.Size(m)
}
func (m *PromQL) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL proto.InternalMessageInfo

type PromQL_InstantQueryRequest struct {
	Query                string   `protobuf:"bytes,1,opt,name=query,proto3" json:"query,omitempty"`
	Time                 string   `protobuf:"bytes,2,opt,name=time,proto3" json:"time,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PromQL_InstantQueryRequest) Reset()         { *m = PromQL_InstantQueryRequest{} }
func (m *PromQL_InstantQueryRequest) String() string { return proto.CompactTextString(m) }
func (*PromQL_InstantQueryRequest) ProtoMessage()    {}
func (*PromQL_InstantQueryRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0, 0}
}

func (m *PromQL_InstantQueryRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL_InstantQueryRequest.Unmarshal(m, b)
}
func (m *PromQL_InstantQueryRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL_InstantQueryRequest.Marshal(b, m, deterministic)
}
func (m *PromQL_InstantQueryRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL_InstantQueryRequest.Merge(m, src)
}
func (m *PromQL_InstantQueryRequest) XXX_Size() int {
	return xxx_messageInfo_PromQL_InstantQueryRequest.Size(m)
}
func (m *PromQL_InstantQueryRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL_InstantQueryRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL_InstantQueryRequest proto.InternalMessageInfo

func (m *PromQL_InstantQueryRequest) GetQuery() string {
	if m != nil {
		return m.Query
	}
	return ""
}

func (m *PromQL_InstantQueryRequest) GetTime() string {
	if m != nil {
		return m.Time
	}
	return ""
}

type PromQL_RangeQueryRequest struct {
	Query                string   `protobuf:"bytes,1,opt,name=query,proto3" json:"query,omitempty"`
	Start                string   `protobuf:"bytes,2,opt,name=start,proto3" json:"start,omitempty"`
	End                  string   `protobuf:"bytes,3,opt,name=end,proto3" json:"end,omitempty"`
	Step                 string   `protobuf:"bytes,4,opt,name=step,proto3" json:"step,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PromQL_RangeQueryRequest) Reset()         { *m = PromQL_RangeQueryRequest{} }
func (m *PromQL_RangeQueryRequest) String() string { return proto.CompactTextString(m) }
func (*PromQL_RangeQueryRequest) ProtoMessage()    {}
func (*PromQL_RangeQueryRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0, 1}
}

func (m *PromQL_RangeQueryRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL_RangeQueryRequest.Unmarshal(m, b)
}
func (m *PromQL_RangeQueryRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL_RangeQueryRequest.Marshal(b, m, deterministic)
}
func (m *PromQL_RangeQueryRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL_RangeQueryRequest.Merge(m, src)
}
func (m *PromQL_RangeQueryRequest) XXX_Size() int {
	return xxx_messageInfo_PromQL_RangeQueryRequest.Size(m)
}
func (m *PromQL_RangeQueryRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL_RangeQueryRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL_RangeQueryRequest proto.InternalMessageInfo

func (m *PromQL_RangeQueryRequest) GetQuery() string {
	if m != nil {
		return m.Query
	}
	return ""
}

func (m *PromQL_RangeQueryRequest) GetStart() string {
	if m != nil {
		return m.Start
	}
	return ""
}

func (m *PromQL_RangeQueryRequest) GetEnd() string {
	if m != nil {
		return m.End
	}
	return ""
}

func (m *PromQL_RangeQueryRequest) GetStep() string {
	if m != nil {
		return m.Step
	}
	return ""
}

type PromQL_InstantQueryResult struct {
	// Types that are valid to be assigned to Result:
	//	*PromQL_InstantQueryResult_Scalar
	//	*PromQL_InstantQueryResult_Vector
	//	*PromQL_InstantQueryResult_Matrix
	Result               isPromQL_InstantQueryResult_Result `protobuf_oneof:"Result"`
	XXX_NoUnkeyedLiteral struct{}                           `json:"-"`
	XXX_unrecognized     []byte                             `json:"-"`
	XXX_sizecache        int32                              `json:"-"`
}

func (m *PromQL_InstantQueryResult) Reset()         { *m = PromQL_InstantQueryResult{} }
func (m *PromQL_InstantQueryResult) String() string { return proto.CompactTextString(m) }
func (*PromQL_InstantQueryResult) ProtoMessage()    {}
func (*PromQL_InstantQueryResult) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0, 2}
}

func (m *PromQL_InstantQueryResult) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL_InstantQueryResult.Unmarshal(m, b)
}
func (m *PromQL_InstantQueryResult) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL_InstantQueryResult.Marshal(b, m, deterministic)
}
func (m *PromQL_InstantQueryResult) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL_InstantQueryResult.Merge(m, src)
}
func (m *PromQL_InstantQueryResult) XXX_Size() int {
	return xxx_messageInfo_PromQL_InstantQueryResult.Size(m)
}
func (m *PromQL_InstantQueryResult) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL_InstantQueryResult.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL_InstantQueryResult proto.InternalMessageInfo

type isPromQL_InstantQueryResult_Result interface {
	isPromQL_InstantQueryResult_Result()
}

type PromQL_InstantQueryResult_Scalar struct {
	Scalar *PromQL_Scalar `protobuf:"bytes,1,opt,name=scalar,proto3,oneof"`
}

type PromQL_InstantQueryResult_Vector struct {
	Vector *PromQL_Vector `protobuf:"bytes,2,opt,name=vector,proto3,oneof"`
}

type PromQL_InstantQueryResult_Matrix struct {
	Matrix *PromQL_Matrix `protobuf:"bytes,3,opt,name=matrix,proto3,oneof"`
}

func (*PromQL_InstantQueryResult_Scalar) isPromQL_InstantQueryResult_Result() {}

func (*PromQL_InstantQueryResult_Vector) isPromQL_InstantQueryResult_Result() {}

func (*PromQL_InstantQueryResult_Matrix) isPromQL_InstantQueryResult_Result() {}

func (m *PromQL_InstantQueryResult) GetResult() isPromQL_InstantQueryResult_Result {
	if m != nil {
		return m.Result
	}
	return nil
}

func (m *PromQL_InstantQueryResult) GetScalar() *PromQL_Scalar {
	if x, ok := m.GetResult().(*PromQL_InstantQueryResult_Scalar); ok {
		return x.Scalar
	}
	return nil
}

func (m *PromQL_InstantQueryResult) GetVector() *PromQL_Vector {
	if x, ok := m.GetResult().(*PromQL_InstantQueryResult_Vector); ok {
		return x.Vector
	}
	return nil
}

func (m *PromQL_InstantQueryResult) GetMatrix() *PromQL_Matrix {
	if x, ok := m.GetResult().(*PromQL_InstantQueryResult_Matrix); ok {
		return x.Matrix
	}
	return nil
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*PromQL_InstantQueryResult) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _PromQL_InstantQueryResult_OneofMarshaler, _PromQL_InstantQueryResult_OneofUnmarshaler, _PromQL_InstantQueryResult_OneofSizer, []interface{}{
		(*PromQL_InstantQueryResult_Scalar)(nil),
		(*PromQL_InstantQueryResult_Vector)(nil),
		(*PromQL_InstantQueryResult_Matrix)(nil),
	}
}

func _PromQL_InstantQueryResult_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*PromQL_InstantQueryResult)
	// Result
	switch x := m.Result.(type) {
	case *PromQL_InstantQueryResult_Scalar:
		b.EncodeVarint(1<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Scalar); err != nil {
			return err
		}
	case *PromQL_InstantQueryResult_Vector:
		b.EncodeVarint(2<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Vector); err != nil {
			return err
		}
	case *PromQL_InstantQueryResult_Matrix:
		b.EncodeVarint(3<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Matrix); err != nil {
			return err
		}
	case nil:
	default:
		return fmt.Errorf("PromQL_InstantQueryResult.Result has unexpected type %T", x)
	}
	return nil
}

func _PromQL_InstantQueryResult_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*PromQL_InstantQueryResult)
	switch tag {
	case 1: // Result.scalar
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(PromQL_Scalar)
		err := b.DecodeMessage(msg)
		m.Result = &PromQL_InstantQueryResult_Scalar{msg}
		return true, err
	case 2: // Result.vector
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(PromQL_Vector)
		err := b.DecodeMessage(msg)
		m.Result = &PromQL_InstantQueryResult_Vector{msg}
		return true, err
	case 3: // Result.matrix
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(PromQL_Matrix)
		err := b.DecodeMessage(msg)
		m.Result = &PromQL_InstantQueryResult_Matrix{msg}
		return true, err
	default:
		return false, nil
	}
}

func _PromQL_InstantQueryResult_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*PromQL_InstantQueryResult)
	// Result
	switch x := m.Result.(type) {
	case *PromQL_InstantQueryResult_Scalar:
		s := proto.Size(x.Scalar)
		n += 1 // tag and wire
		n += proto.SizeVarint(uint64(s))
		n += s
	case *PromQL_InstantQueryResult_Vector:
		s := proto.Size(x.Vector)
		n += 1 // tag and wire
		n += proto.SizeVarint(uint64(s))
		n += s
	case *PromQL_InstantQueryResult_Matrix:
		s := proto.Size(x.Matrix)
		n += 1 // tag and wire
		n += proto.SizeVarint(uint64(s))
		n += s
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type PromQL_RangeQueryResult struct {
	// Types that are valid to be assigned to Result:
	//	*PromQL_RangeQueryResult_Matrix
	Result               isPromQL_RangeQueryResult_Result `protobuf_oneof:"Result"`
	XXX_NoUnkeyedLiteral struct{}                         `json:"-"`
	XXX_unrecognized     []byte                           `json:"-"`
	XXX_sizecache        int32                            `json:"-"`
}

func (m *PromQL_RangeQueryResult) Reset()         { *m = PromQL_RangeQueryResult{} }
func (m *PromQL_RangeQueryResult) String() string { return proto.CompactTextString(m) }
func (*PromQL_RangeQueryResult) ProtoMessage()    {}
func (*PromQL_RangeQueryResult) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0, 3}
}

func (m *PromQL_RangeQueryResult) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL_RangeQueryResult.Unmarshal(m, b)
}
func (m *PromQL_RangeQueryResult) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL_RangeQueryResult.Marshal(b, m, deterministic)
}
func (m *PromQL_RangeQueryResult) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL_RangeQueryResult.Merge(m, src)
}
func (m *PromQL_RangeQueryResult) XXX_Size() int {
	return xxx_messageInfo_PromQL_RangeQueryResult.Size(m)
}
func (m *PromQL_RangeQueryResult) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL_RangeQueryResult.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL_RangeQueryResult proto.InternalMessageInfo

type isPromQL_RangeQueryResult_Result interface {
	isPromQL_RangeQueryResult_Result()
}

type PromQL_RangeQueryResult_Matrix struct {
	Matrix *PromQL_Matrix `protobuf:"bytes,1,opt,name=matrix,proto3,oneof"`
}

func (*PromQL_RangeQueryResult_Matrix) isPromQL_RangeQueryResult_Result() {}

func (m *PromQL_RangeQueryResult) GetResult() isPromQL_RangeQueryResult_Result {
	if m != nil {
		return m.Result
	}
	return nil
}

func (m *PromQL_RangeQueryResult) GetMatrix() *PromQL_Matrix {
	if x, ok := m.GetResult().(*PromQL_RangeQueryResult_Matrix); ok {
		return x.Matrix
	}
	return nil
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*PromQL_RangeQueryResult) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _PromQL_RangeQueryResult_OneofMarshaler, _PromQL_RangeQueryResult_OneofUnmarshaler, _PromQL_RangeQueryResult_OneofSizer, []interface{}{
		(*PromQL_RangeQueryResult_Matrix)(nil),
	}
}

func _PromQL_RangeQueryResult_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*PromQL_RangeQueryResult)
	// Result
	switch x := m.Result.(type) {
	case *PromQL_RangeQueryResult_Matrix:
		b.EncodeVarint(1<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Matrix); err != nil {
			return err
		}
	case nil:
	default:
		return fmt.Errorf("PromQL_RangeQueryResult.Result has unexpected type %T", x)
	}
	return nil
}

func _PromQL_RangeQueryResult_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*PromQL_RangeQueryResult)
	switch tag {
	case 1: // Result.matrix
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(PromQL_Matrix)
		err := b.DecodeMessage(msg)
		m.Result = &PromQL_RangeQueryResult_Matrix{msg}
		return true, err
	default:
		return false, nil
	}
}

func _PromQL_RangeQueryResult_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*PromQL_RangeQueryResult)
	// Result
	switch x := m.Result.(type) {
	case *PromQL_RangeQueryResult_Matrix:
		s := proto.Size(x.Matrix)
		n += 1 // tag and wire
		n += proto.SizeVarint(uint64(s))
		n += s
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type PromQL_Scalar struct {
	Time                 string   `protobuf:"bytes,1,opt,name=time,proto3" json:"time,omitempty"`
	Value                float64  `protobuf:"fixed64,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PromQL_Scalar) Reset()         { *m = PromQL_Scalar{} }
func (m *PromQL_Scalar) String() string { return proto.CompactTextString(m) }
func (*PromQL_Scalar) ProtoMessage()    {}
func (*PromQL_Scalar) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0, 4}
}

func (m *PromQL_Scalar) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL_Scalar.Unmarshal(m, b)
}
func (m *PromQL_Scalar) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL_Scalar.Marshal(b, m, deterministic)
}
func (m *PromQL_Scalar) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL_Scalar.Merge(m, src)
}
func (m *PromQL_Scalar) XXX_Size() int {
	return xxx_messageInfo_PromQL_Scalar.Size(m)
}
func (m *PromQL_Scalar) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL_Scalar.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL_Scalar proto.InternalMessageInfo

func (m *PromQL_Scalar) GetTime() string {
	if m != nil {
		return m.Time
	}
	return ""
}

func (m *PromQL_Scalar) GetValue() float64 {
	if m != nil {
		return m.Value
	}
	return 0
}

type PromQL_Vector struct {
	Samples              []*PromQL_Sample `protobuf:"bytes,1,rep,name=samples,proto3" json:"samples,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *PromQL_Vector) Reset()         { *m = PromQL_Vector{} }
func (m *PromQL_Vector) String() string { return proto.CompactTextString(m) }
func (*PromQL_Vector) ProtoMessage()    {}
func (*PromQL_Vector) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0, 5}
}

func (m *PromQL_Vector) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL_Vector.Unmarshal(m, b)
}
func (m *PromQL_Vector) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL_Vector.Marshal(b, m, deterministic)
}
func (m *PromQL_Vector) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL_Vector.Merge(m, src)
}
func (m *PromQL_Vector) XXX_Size() int {
	return xxx_messageInfo_PromQL_Vector.Size(m)
}
func (m *PromQL_Vector) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL_Vector.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL_Vector proto.InternalMessageInfo

func (m *PromQL_Vector) GetSamples() []*PromQL_Sample {
	if m != nil {
		return m.Samples
	}
	return nil
}

type PromQL_Point struct {
	Time                 string   `protobuf:"bytes,1,opt,name=time,proto3" json:"time,omitempty"`
	Value                float64  `protobuf:"fixed64,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PromQL_Point) Reset()         { *m = PromQL_Point{} }
func (m *PromQL_Point) String() string { return proto.CompactTextString(m) }
func (*PromQL_Point) ProtoMessage()    {}
func (*PromQL_Point) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0, 6}
}

func (m *PromQL_Point) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL_Point.Unmarshal(m, b)
}
func (m *PromQL_Point) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL_Point.Marshal(b, m, deterministic)
}
func (m *PromQL_Point) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL_Point.Merge(m, src)
}
func (m *PromQL_Point) XXX_Size() int {
	return xxx_messageInfo_PromQL_Point.Size(m)
}
func (m *PromQL_Point) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL_Point.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL_Point proto.InternalMessageInfo

func (m *PromQL_Point) GetTime() string {
	if m != nil {
		return m.Time
	}
	return ""
}

func (m *PromQL_Point) GetValue() float64 {
	if m != nil {
		return m.Value
	}
	return 0
}

type PromQL_Sample struct {
	Metric               map[string]string `protobuf:"bytes,1,rep,name=metric,proto3" json:"metric,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Point                *PromQL_Point     `protobuf:"bytes,2,opt,name=point,proto3" json:"point,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *PromQL_Sample) Reset()         { *m = PromQL_Sample{} }
func (m *PromQL_Sample) String() string { return proto.CompactTextString(m) }
func (*PromQL_Sample) ProtoMessage()    {}
func (*PromQL_Sample) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0, 7}
}

func (m *PromQL_Sample) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL_Sample.Unmarshal(m, b)
}
func (m *PromQL_Sample) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL_Sample.Marshal(b, m, deterministic)
}
func (m *PromQL_Sample) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL_Sample.Merge(m, src)
}
func (m *PromQL_Sample) XXX_Size() int {
	return xxx_messageInfo_PromQL_Sample.Size(m)
}
func (m *PromQL_Sample) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL_Sample.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL_Sample proto.InternalMessageInfo

func (m *PromQL_Sample) GetMetric() map[string]string {
	if m != nil {
		return m.Metric
	}
	return nil
}

func (m *PromQL_Sample) GetPoint() *PromQL_Point {
	if m != nil {
		return m.Point
	}
	return nil
}

type PromQL_Matrix struct {
	Series               []*PromQL_Series `protobuf:"bytes,1,rep,name=series,proto3" json:"series,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *PromQL_Matrix) Reset()         { *m = PromQL_Matrix{} }
func (m *PromQL_Matrix) String() string { return proto.CompactTextString(m) }
func (*PromQL_Matrix) ProtoMessage()    {}
func (*PromQL_Matrix) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0, 8}
}

func (m *PromQL_Matrix) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL_Matrix.Unmarshal(m, b)
}
func (m *PromQL_Matrix) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL_Matrix.Marshal(b, m, deterministic)
}
func (m *PromQL_Matrix) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL_Matrix.Merge(m, src)
}
func (m *PromQL_Matrix) XXX_Size() int {
	return xxx_messageInfo_PromQL_Matrix.Size(m)
}
func (m *PromQL_Matrix) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL_Matrix.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL_Matrix proto.InternalMessageInfo

func (m *PromQL_Matrix) GetSeries() []*PromQL_Series {
	if m != nil {
		return m.Series
	}
	return nil
}

type PromQL_Series struct {
	Metric               map[string]string `protobuf:"bytes,1,rep,name=metric,proto3" json:"metric,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Points               []*PromQL_Point   `protobuf:"bytes,2,rep,name=points,proto3" json:"points,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *PromQL_Series) Reset()         { *m = PromQL_Series{} }
func (m *PromQL_Series) String() string { return proto.CompactTextString(m) }
func (*PromQL_Series) ProtoMessage()    {}
func (*PromQL_Series) Descriptor() ([]byte, []int) {
	return fileDescriptor_0808f1dc738840d6, []int{0, 9}
}

func (m *PromQL_Series) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PromQL_Series.Unmarshal(m, b)
}
func (m *PromQL_Series) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PromQL_Series.Marshal(b, m, deterministic)
}
func (m *PromQL_Series) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PromQL_Series.Merge(m, src)
}
func (m *PromQL_Series) XXX_Size() int {
	return xxx_messageInfo_PromQL_Series.Size(m)
}
func (m *PromQL_Series) XXX_DiscardUnknown() {
	xxx_messageInfo_PromQL_Series.DiscardUnknown(m)
}

var xxx_messageInfo_PromQL_Series proto.InternalMessageInfo

func (m *PromQL_Series) GetMetric() map[string]string {
	if m != nil {
		return m.Metric
	}
	return nil
}

func (m *PromQL_Series) GetPoints() []*PromQL_Point {
	if m != nil {
		return m.Points
	}
	return nil
}

func init() {
	proto.RegisterType((*PromQL)(nil), "logcache.v1.PromQL")
	proto.RegisterType((*PromQL_InstantQueryRequest)(nil), "logcache.v1.PromQL.InstantQueryRequest")
	proto.RegisterType((*PromQL_RangeQueryRequest)(nil), "logcache.v1.PromQL.RangeQueryRequest")
	proto.RegisterType((*PromQL_InstantQueryResult)(nil), "logcache.v1.PromQL.InstantQueryResult")
	proto.RegisterType((*PromQL_RangeQueryResult)(nil), "logcache.v1.PromQL.RangeQueryResult")
	proto.RegisterType((*PromQL_Scalar)(nil), "logcache.v1.PromQL.Scalar")
	proto.RegisterType((*PromQL_Vector)(nil), "logcache.v1.PromQL.Vector")
	proto.RegisterType((*PromQL_Point)(nil), "logcache.v1.PromQL.Point")
	proto.RegisterType((*PromQL_Sample)(nil), "logcache.v1.PromQL.Sample")
	proto.RegisterMapType((map[string]string)(nil), "logcache.v1.PromQL.Sample.MetricEntry")
	proto.RegisterType((*PromQL_Matrix)(nil), "logcache.v1.PromQL.Matrix")
	proto.RegisterType((*PromQL_Series)(nil), "logcache.v1.PromQL.Series")
	proto.RegisterMapType((map[string]string)(nil), "logcache.v1.PromQL.Series.MetricEntry")
}

func init() { proto.RegisterFile("promql.proto", fileDescriptor_0808f1dc738840d6) }

var fileDescriptor_0808f1dc738840d6 = []byte{
	// 533 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xa4, 0x94, 0xc1, 0x6e, 0x13, 0x31,
	0x10, 0x86, 0x71, 0xda, 0x98, 0x76, 0xd2, 0x8a, 0xe2, 0xb6, 0xd2, 0x62, 0x38, 0x54, 0x15, 0x14,
	0x4e, 0x1b, 0x25, 0x70, 0x00, 0x84, 0x8a, 0x84, 0x84, 0x04, 0x12, 0x95, 0x5a, 0x23, 0xf5, 0x8a,
	0xcc, 0x62, 0x85, 0x15, 0xbb, 0xeb, 0x8d, 0xed, 0xac, 0xe8, 0x95, 0x57, 0xe0, 0xca, 0x63, 0x20,
	0xce, 0xbc, 0x03, 0xaf, 0xc0, 0x7b, 0x80, 0x3c, 0x76, 0x68, 0x22, 0x36, 0xa4, 0xc0, 0x6d, 0xbc,
	0xfa, 0xfe, 0xf9, 0x67, 0xc6, 0xde, 0x81, 0x8d, 0xda, 0xe8, 0x72, 0x5c, 0xa4, 0xb5, 0xd1, 0x4e,
	0xb3, 0x5e, 0xa1, 0x47, 0x99, 0xcc, 0xde, 0xaa, 0xb4, 0x19, 0xf0, 0x1b, 0x23, 0xad, 0x47, 0x85,
	0xea, 0xcb, 0x3a, 0xef, 0xcb, 0xaa, 0xd2, 0x4e, 0xba, 0x5c, 0x57, 0x36, 0xa0, 0xfb, 0x9f, 0xd6,
	0x80, 0x1e, 0x1b, 0x5d, 0x9e, 0xbc, 0xe0, 0x8f, 0x61, 0xfb, 0x79, 0x65, 0x9d, 0xac, 0xdc, 0xc9,
	0x44, 0x99, 0x33, 0xa1, 0xc6, 0x13, 0x65, 0x1d, 0xdb, 0x81, 0xee, 0xd8, 0x9f, 0x13, 0xb2, 0x47,
	0xee, 0xac, 0x8b, 0x70, 0x60, 0x0c, 0x56, 0x5d, 0x5e, 0xaa, 0xa4, 0x83, 0x1f, 0x31, 0xe6, 0x0a,
	0xae, 0x0a, 0x59, 0x8d, 0xd4, 0x05, 0xe4, 0x3b, 0xd0, 0xb5, 0x4e, 0x1a, 0x17, 0xf5, 0xe1, 0xc0,
	0xb6, 0x60, 0x45, 0x55, 0x6f, 0x92, 0x15, 0xfc, 0xe6, 0x43, 0x6f, 0x63, 0x9d, 0xaa, 0x93, 0xd5,
	0x60, 0xe3, 0x63, 0xfe, 0x95, 0x00, 0x9b, 0x2f, 0xd4, 0x4e, 0x0a, 0xc7, 0xee, 0x01, 0xb5, 0x99,
	0x2c, 0xa4, 0x41, 0xa7, 0xde, 0x90, 0xa7, 0x33, 0x53, 0x48, 0x43, 0x8f, 0xe9, 0x4b, 0x24, 0x9e,
	0x5d, 0x12, 0x91, 0xf5, 0xaa, 0x46, 0x65, 0x4e, 0x1b, 0xac, 0x64, 0x81, 0xea, 0x14, 0x09, 0xaf,
	0x0a, 0xac, 0x57, 0x95, 0xd2, 0x99, 0xfc, 0x3d, 0xd6, 0xba, 0x40, 0x75, 0x84, 0x84, 0x57, 0x05,
	0xf6, 0xc9, 0x1a, 0xd0, 0x50, 0x2b, 0x17, 0xb0, 0x35, 0x3b, 0xa9, 0x69, 0xfd, 0x31, 0x27, 0xf9,
	0xa7, 0x9c, 0x43, 0xa0, 0xa1, 0xbb, 0x5f, 0x77, 0x43, 0xce, 0xef, 0xc6, 0x0f, 0xbc, 0x91, 0xc5,
	0x24, 0x5c, 0x18, 0x11, 0xe1, 0xc0, 0x0f, 0x81, 0x9e, 0x4e, 0x3b, 0xba, 0x6c, 0x65, 0x59, 0x17,
	0xca, 0x26, 0x64, 0x6f, 0x65, 0xe1, 0xf8, 0x10, 0x11, 0x53, 0x94, 0x0f, 0xa0, 0x7b, 0xac, 0xf3,
	0xca, 0xfd, 0x85, 0xe5, 0x67, 0x02, 0x34, 0xa4, 0x61, 0x87, 0x40, 0x4b, 0xe5, 0x4c, 0x9e, 0x45,
	0xcb, 0x83, 0xc5, 0x96, 0xe9, 0x11, 0x82, 0x4f, 0x2b, 0x67, 0xce, 0x44, 0x54, 0xb1, 0x3e, 0x74,
	0x6b, 0xef, 0x1e, 0xaf, 0xee, 0x5a, 0x9b, 0x1c, 0xcb, 0x13, 0x81, 0xe3, 0x0f, 0xa0, 0x37, 0x93,
	0xc7, 0x3f, 0xb7, 0x77, 0x6a, 0xfa, 0x30, 0x7d, 0x38, 0x5f, 0xf2, 0x7a, 0x2c, 0xf9, 0x61, 0xe7,
	0x3e, 0xe1, 0x8f, 0x80, 0x86, 0xd9, 0xb3, 0x21, 0x50, 0xab, 0x4c, 0xbe, 0x64, 0x50, 0x48, 0x88,
	0x48, 0xf2, 0x2f, 0xbe, 0x69, 0x0c, 0x2f, 0xd8, 0x34, 0xb2, 0xad, 0x4d, 0x0f, 0x80, 0x62, 0x33,
	0x36, 0xe9, 0xa0, 0xfe, 0x0f, 0x5d, 0x47, 0xf0, 0x3f, 0xda, 0x1e, 0xfe, 0x20, 0xb0, 0x19, 0x72,
	0xfa, 0xa7, 0x9a, 0x2b, 0xc3, 0x1a, 0xd8, 0x98, 0xfd, 0xf9, 0xd8, 0xed, 0x36, 0xff, 0x96, 0x3d,
	0xc2, 0x0f, 0x96, 0x83, 0xfe, 0x1d, 0xef, 0xef, 0x7e, 0xf8, 0xf6, 0xfd, 0x63, 0xe7, 0x0a, 0xdb,
	0xc4, 0x8d, 0xd5, 0x0c, 0xfa, 0x61, 0x63, 0x34, 0x00, 0xe7, 0xbf, 0x0c, 0xbb, 0xd5, 0x96, 0xec,
	0xb7, 0xe5, 0xc3, 0x6f, 0x2e, 0xc3, 0xd0, 0xf1, 0x3a, 0x3a, 0xee, 0xb2, 0xed, 0x39, 0xc7, 0x57,
	0xc6, 0x73, 0xaf, 0x29, 0xee, 0xc9, 0xbb, 0x3f, 0x03, 0x00, 0x00, 0xff, 0xff, 0xd8, 0xdc, 0x69,
	0xcb, 0x62, 0x05, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// PromQLQuerierClient is the client API for PromQLQuerier service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type PromQLQuerierClient interface {
	InstantQuery(ctx context.Context, in *PromQL_InstantQueryRequest, opts ...grpc.CallOption) (*PromQL_InstantQueryResult, error)
	RangeQuery(ctx context.Context, in *PromQL_RangeQueryRequest, opts ...grpc.CallOption) (*PromQL_RangeQueryResult, error)
}

type promQLQuerierClient struct {
	cc *grpc.ClientConn
}

func NewPromQLQuerierClient(cc *grpc.ClientConn) PromQLQuerierClient {
	return &promQLQuerierClient{cc}
}

func (c *promQLQuerierClient) InstantQuery(ctx context.Context, in *PromQL_InstantQueryRequest, opts ...grpc.CallOption) (*PromQL_InstantQueryResult, error) {
	out := new(PromQL_InstantQueryResult)
	err := c.cc.Invoke(ctx, "/logcache.v1.PromQLQuerier/InstantQuery", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *promQLQuerierClient) RangeQuery(ctx context.Context, in *PromQL_RangeQueryRequest, opts ...grpc.CallOption) (*PromQL_RangeQueryResult, error) {
	out := new(PromQL_RangeQueryResult)
	err := c.cc.Invoke(ctx, "/logcache.v1.PromQLQuerier/RangeQuery", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PromQLQuerierServer is the server API for PromQLQuerier service.
type PromQLQuerierServer interface {
	InstantQuery(context.Context, *PromQL_InstantQueryRequest) (*PromQL_InstantQueryResult, error)
	RangeQuery(context.Context, *PromQL_RangeQueryRequest) (*PromQL_RangeQueryResult, error)
}

func RegisterPromQLQuerierServer(s *grpc.Server, srv PromQLQuerierServer) {
	s.RegisterService(&_PromQLQuerier_serviceDesc, srv)
}

func _PromQLQuerier_InstantQuery_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PromQL_InstantQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PromQLQuerierServer).InstantQuery(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/logcache.v1.PromQLQuerier/InstantQuery",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PromQLQuerierServer).InstantQuery(ctx, req.(*PromQL_InstantQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PromQLQuerier_RangeQuery_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PromQL_RangeQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PromQLQuerierServer).RangeQuery(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/logcache.v1.PromQLQuerier/RangeQuery",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PromQLQuerierServer).RangeQuery(ctx, req.(*PromQL_RangeQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _PromQLQuerier_serviceDesc = grpc.ServiceDesc{
	ServiceName: "logcache.v1.PromQLQuerier",
	HandlerType: (*PromQLQuerierServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "InstantQuery",
			Handler:    _PromQLQuerier_InstantQuery_Handler,
		},
		{
			MethodName: "RangeQuery",
			Handler:    _PromQLQuerier_RangeQuery_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "promql.proto",
}
