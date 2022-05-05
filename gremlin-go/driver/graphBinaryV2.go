package gremlingo

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"math"
	"math/big"
	"reflect"
	"time"
)

func readTemp(data *[]byte, i *int, len int) *[]byte {
	tmp := make([]byte, len)
	for j := 0; j < len; j++ {
		tmp[j] = (*data)[j+*i]
	}
	*i += len
	return &tmp
}

// Primitive
func readBoolean(data *[]byte, i *int) interface{} {
	return readByte(data, i) != 0
}

func readByte(data *[]byte, i *int) interface{} {
	*i++
	return (*data)[*i-1]
}

func readShort(data *[]byte, i *int) interface{} {
	return int16(binary.BigEndian.Uint16(*readTemp(data, i, 2)))
}

func readInt(data *[]byte, i *int) interface{} {
	return int32(binary.BigEndian.Uint32(*readTemp(data, i, 4)))
}

func readLong(data *[]byte, i *int) interface{} {
	return int64(binary.BigEndian.Uint64(*readTemp(data, i, 8)))
}

func readBigInt(data *[]byte, i *int) interface{} {
	sz := readInt(data, i).(int32)
	b := readTemp(data, i, int(sz))

	var newBigInt = big.NewInt(0).SetBytes(*b)
	var one = big.NewInt(1)
	if len(*b) == 0 {
		return newBigInt
	}
	// If the first bit in the first element of the byte array is a 1, we need to interpret the byte array as a two's complement representation
	if (*b)[0]&0x80 == 0x00 {
		newBigInt.SetBytes(*b)
		return newBigInt
	}
	// Undo two's complement to byte array and set negative boolean to true
	length := uint((len(*b)*8)/8+1) * 8
	b2 := new(big.Int).Sub(newBigInt, new(big.Int).Lsh(one, length)).Bytes()

	// Strip the resulting 0xff byte at the start of array
	b2 = b2[1:]

	// Strip any redundant 0x00 byte at the start of array
	if b2[0] == 0x00 {
		b2 = b2[1:]
	}
	newBigInt = big.NewInt(0)
	newBigInt.SetBytes(b2)
	newBigInt.Neg(newBigInt)
	return newBigInt
}

func readUint32(data *[]byte, i *int) interface{} {
	return binary.BigEndian.Uint32(*readTemp(data, i, 4))
}

func readFloat(data *[]byte, i *int) interface{} {
	return math.Float32frombits(binary.BigEndian.Uint32(*readTemp(data, i, 4)))
}

func readDouble(data *[]byte, i *int) interface{} {
	return math.Float64frombits(binary.BigEndian.Uint64(*readTemp(data, i, 8)))
}

func readString2(data *[]byte, i *int) interface{} {
	sz := int(readUint32(data, i).(uint32))
	if sz == 0 {
		return ""
	}

	tmp := make([]byte, sz)
	for j := 0; j < sz; j++ {
		tmp[j] = (*data)[j+*i]
	}
	*i += sz
	return string(tmp)
}

func readDataType(data *[]byte, i *int) dataType {
	return dataType(readByte(data, i).(byte))
}

// Composite
func readList(data *[]byte, i *int) interface{} {
	// listEnd := time.Now()
	sz := readInt(data, i).(int32)
	var valList []interface{}
	for j := int32(0); j < sz; j++ {
		valList = append(valList, readFullyQualifiedNullable(data, i, true))
	}
	// listEnd := time.Now()
	// println("list: ", mapEnd.Sub(mapStart).Seconds())
	return valList
}

func readMap2(data *[]byte, i *int) interface{} {
	// mapStart := time.Now()
	sz := readUint32(data, i).(uint32)
	var mapData = make(map[interface{}]interface{})
	for j := uint32(0); j < sz; j++ {
		k := readFullyQualifiedNullable(data, i, true)
		v := readFullyQualifiedNullable(data, i, true)
		if k == nil {
			mapData[nil] = v
		} else {
			switch reflect.TypeOf(k).Kind() {
			case reflect.Map:
				mapData[&k] = v
				break
			case reflect.Slice:
				mapData[fmt.Sprint(k)] = v
				break
			default:
				mapData[k] = v
				break
			}
		}
	}
	return mapData
}

func readMapUnqualified2(data *[]byte, i *int) interface{} {
	sz := readUint32(data, i).(uint32)
	var mapData = make(map[string]interface{})
	for j := uint32(0); j < sz; j++ {
		keyDataType := readDataType(data, i)
		if keyDataType != stringType {
			return nil // , newError(err0703ReadMapNonStringKeyError)
		}

		// Skip nullable, key must be present
		*i++

		k := readString2(data, i).(string)
		mapData[k] = readFullyQualifiedNullable(data, i, true)
	}
	return mapData //, nil
}

func readSet(data *[]byte, i *int) interface{} {
	return NewSimpleSet(readList(data, i).([]interface{})...)
}

func readUuid(data *[]byte, i *int) interface{} {
	id, _ := uuid.FromBytes(*readTemp(data, i, 16))
	return id
}

func timeReader2(data *[]byte, i *int) interface{} {
	return time.UnixMilli(readLong(data, i).(int64))
}

func durationReader2(data *[]byte, i *int) interface{} {
	return time.Duration(readLong(data, i).(int64)*int64(time.Second) + int64(readInt(data, i).(int32)))
}

// Graph

// {fully qualified id}{unqualified label}
func vertexReader2(data *[]byte, i *int) interface{} {
	return vertexReaderNullByte(data, i, true)
}

// {fully qualified id}{unqualified label}{[unused null byte]}
func vertexReaderNullByte(data *[]byte, i *int, unusedByte bool) interface{} {
	v := new(Vertex)
	v.Id = readFullyQualifiedNullable(data, i, true)
	v.Label = readUnqualified(data, i, stringType, false).(string)
	if unusedByte {
		*i++
	}
	return v
}

// {fully qualified id}{unqualified label}{in vertex w/o null byte}{out vertex}{unused null byte}{unused null byte}
func edgeReader2(data *[]byte, i *int) interface{} {
	e := new(Edge)
	e.Id = readFullyQualifiedNullable(data, i, true)
	e.Label = readUnqualified(data, i, stringType, false).(string)
	e.InV = *vertexReaderNullByte(data, i, false).(*Vertex)
	e.OutV = *vertexReaderNullByte(data, i, false).(*Vertex)
	*i += 2
	return e
}

// {unqualified key}{fully qualified value}{null byte}
func propertyReader2(data *[]byte, i *int) interface{} {
	p := new(Property)
	p.Key = readUnqualified(data, i, stringType, false).(string)
	p.Value = readFullyQualifiedNullable(data, i, true)
	*i++
	return p
}

// {fully qualified id}{unqualified label}{fully qualified value}{null byte}{null byte}
func vertexPropertyReader2(data *[]byte, i *int) interface{} {
	vp := new(VertexProperty)
	vp.Id = readFullyQualifiedNullable(data, i, true)
	vp.Label = readUnqualified(data, i, stringType, false).(string)
	vp.Value = readFullyQualifiedNullable(data, i, true)
	*i += 2
	return vp
}

// {list of set of strings}{list of fully qualified objects}
func pathReader2(data *[]byte, i *int) interface{} {
	path := new(Path)
	newLabels := readFullyQualifiedNullable(data, i, true)
	for _, param := range newLabels.([]interface{}) {
		path.Labels = append(path.Labels, param.(*SimpleSet))
	}
	path.Objects = readFullyQualifiedNullable(data, i, true).([]interface{})
	return path
}

// {bulk int}{fully qualified value}
func traverserReader(data *[]byte, i *int) interface{} {
	traverser := new(Traverser)
	traverser.bulk = readLong(data, i).(int64)
	traverser.value = readFullyQualifiedNullable(data, i, true)
	return traverser
}

// {int32 length}{fully qualified item_0}{int64 repetition_0}...{fully qualified item_n}{int64 repetition_n}
func bulkSetReader2(data *[]byte, i *int) interface{} {
	sz := int(readInt(data, i).(int32))
	var valList []interface{}
	for j := 0; j < sz; j++ {
		val := readFullyQualifiedNullable(data, i, true)
		rep := readLong(data, i).(int64)
		for k := 0; k < int(rep); k++ {
			valList = append(valList, val)
		}
	}
	return valList
}

// {type code (always string so ignore)}{nil code (always false so ignore)}{int32 size}{string enum}
func enumReader2(data *[]byte, i *int) interface{} {
	*i += 2
	return readString2(data, i)
}

// {unqualified key}{fully qualified value}
func bindingReader2(data *[]byte, i *int) interface{} {
	b := new(Binding)
	b.Key = readUnqualified(data, i, stringType, false).(string)
	b.Value = readFullyQualifiedNullable(data, i, true)
	return b
}

func readUnqualified(data *[]byte, i *int, dataTyp dataType, nullable bool) interface{} {
	if nullable && readBoolean(data, i).(bool) {
		return nil
	}
	return deserializers[dataTyp](data, i)
}

func readFullyQualifiedNullable(data *[]byte, i *int, nullable bool) interface{} {
	dataTyp := readDataType(data, i)
	if dataTyp == nullType {
		return nil
	} else if nullable {
		if readByte(data, i).(byte) != byte(0) {
			return nil
		}
	}
	return deserializers[dataTyp](data, i)
}

/*
// writers

// Primitive
func writeBoolean(data interface{}, buffer *[]byte) ([]byte, error) {
	if data.(bool) {
		return append(*buffer, 1), nil
	} else {
		return append(*buffer, 0), nil
	}
}

func writeByte(data interface{}, buffer *[]byte) ([]byte, error) {
	return append(*buffer, data.(byte)), nil
}

func writeShort(data interface{}, buffer *[]byte) ([]byte, error) {
	i := data.(int16)
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint16(tmp, uint16(i))
	return append(*buffer, tmp...), nil
}

func writeInt(data interface{}, buffer *[]byte) ([]byte, error) {
	i := data.(uint32)
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, i)
	return append(*buffer, tmp...), nil
}

func writeLong(data interface{}, buffer *[]byte) ([]byte, error) {
	i := data.(uint64)
	tmp := make([]byte, 8)
	binary.BigEndian.PutUint64(tmp, i)
	return append(*buffer, tmp...), nil
}

func writeBigInt(data interface{}, buffer *[]byte) ([]byte, error) {
	v := data.(*big.Int)
	signedBytes := getSignedBytesFromBigInt(v)
	b, _ := writeInt(int32(len(signedBytes)), buffer)
	return writeBytes(signedBytes, &b), nil
}

func writeFloat(data interface{}, buffer *[]byte) ([]byte, error) {
	v := data.(float32)
	return writeInt(math.Float32bits(v), buffer)
}

func writeDouble(data interface{}, buffer *[]byte) ([]byte, error) {
	v := data.(float64)
	return writeLong(math.Float64bits(v), buffer)
}

func writeString(data interface{}, buffer *[]byte) ([]byte, error) {
	str := data.(string)
	b, _ := writeInt(int32(len(str)), buffer)
	return append(b, []byte(str)...), nil
}

// Composite

// Format: {length}{item_0}...{item_n}
func writeList(data interface{}, buffer *[]byte) ([]byte, error) {
	v := reflect.ValueOf(data)
	valLen := v.Len()
	b, _ := writeInt(int32(valLen), buffer)
	for i := 0; i < valLen; i++ {
		b, _ = write(v.Index(i).Interface(), &b)
	}
	return b, _
}

// Format: {length}{item_0}...{item_n}
func writeMap(data interface{}, buffer *[]byte) ([]byte, error) {
	if data == nil {
		return writeInt(int32(0), buffer)
	}

	v := reflect.ValueOf(data)
	keys := v.MapKeys()
	b, _ := writeInt(int32(len(keys)), buffer)

	for _, k := range keys {
		convKey := k.Convert(v.Type().Key())
		// serialize k
		b, _ = write(k.Interface(), &b)
		// serialize v.MapIndex(c_key)
		val := v.MapIndex(convKey)
		b, _ = write(val.Interface(), &b)
	}
	return b, nil
}

func writeUuid(data interface{}, buffer *[]byte) ([]byte, error) {
	uu := data.(uuid.UUID)
	bytes, _ := uu.MarshalBinary()
	return writeBytes(bytes, buffer), nil
}

func writeTime(data interface{}, buffer *[]byte) ([]byte, error) {
	t := data.(time.Time)
	return writeLong(t, buffer)
}

func writeDuration(data interface{}, buffer *[]byte) ([]byte, error) {
	t := data.(time.Duration)
	sec := int64(t / time.Second)
	nanos := int32(t % time.Second)
	b, _ := writeLong(sec, buffer)
	return writeInt(nanos, &b)
}

// Graph
// Format: {Id}{Label}{properties}
func writeVertex(data interface{}, buffer *[]byte) ([]byte, error) {
	t := data.(time.Duration)
	sec := int64(t / time.Second)
	nanos := int32(t % time.Second)
	b, _ := writeLong(sec, buffer)
	return writeInt(nanos, &b)

	v := data.(*Vertex)
	b, err := write(v.Id, &b)
	if err != nil {
		return nil, err
	}

	// Not fully qualified.
	b, err = writeValue(v.Label, &b, false)
	if err != nil {
		return nil, err
	}

	// Note that as TinkerPop currently send "references" only, properties will always be null
	return writeBytes(nullBytes, &b), nil
}

// utility
func writeBytes(data interface{}, buffer *[]byte) []byte {
	arr := data.([]byte)
	return append(*buffer, arr...)
}

func writeDataType(data interface{}, buffer *[]byte) ([]byte, error) {
	return append(*buffer, data.(uint8)), nil
}

func write(valueObject interface{}, buffer *[]byte) ([]byte, error) {
	if valueObject == nil {
		// return Object of type "unspecified object null" with the value flag set to null.
		return append(*buffer, nullType.getCodeByte(), 0x01), nil
	}

	dataType := getDataType(valueObject)
	typeSerializer := serializers[dataType]

	b, _ := writeDataType(dataType.getCodeByte(), buffer)
	b = typeSerializer(valueObject, &b)

	return b, nil
}

// Writes a value without including type information.
func writeValue(value interface{}, buffer *[]byte, nullable bool) ([]byte, error) {
	if value == nil {
		if !nullable {
			//serializer.logHandler.log(Error, unexpectedNull)
			return nil, newError(err0403WriteValueUnexpectedNullError)
		}
		return writeValueFlagNull(buffer), nil
	}

	typeSerializer, err := getSerializerToWrite(value)
	if err != nil {
		return nil, err
	}
	return typeSerializer(value, buffer)
}

func writeValueFlagNull(buffer *[]byte) []byte {
	b, _ := writeByte(valueFlagNull, buffer)
	return b
}

func writeValueFlagNone(buffer *[]byte) []byte {
	b, _ := writeByte(valueFlagNone, buffer)
	return b
}
*/
