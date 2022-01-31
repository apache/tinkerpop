/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package gremlingo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/big"
	"strings"

	"github.com/google/uuid"
)

const graphBinaryMimeType = "application/vnd.graphbinary-v1.0"

// serializer interface for serializers
type serializer interface {
	serializeMessage(request *request) ([]byte, error)
	deserializeMessage(message []byte) (response, error)
}

// graphBinarySerializer serializes/deserializes message to/from GraphBinary
type graphBinarySerializer struct {
	readerClass *graphBinaryReader
	writerClass *graphBinaryWriter
	mimeType    string `default:"application/vnd.graphbinary-v1.0"`
}

func newGraphBinarySerializer(handler *logHandler) serializer {
	reader := graphBinaryReader{handler}
	writer := graphBinaryWriter{handler}
	return graphBinarySerializer{&reader, &writer, graphBinaryMimeType}
}

const versionByte byte = 0x81

// serializeMessage serializes a request message into GraphBinary
func (gs graphBinarySerializer) serializeMessage(request *request) ([]byte, error) {
	gs.mimeType = graphBinaryMimeType
	finalMessage, err := gs.buildMessage(request, 0x20, gs.mimeType)
	if err != nil {
		return nil, err
	}
	return finalMessage, nil
}

func writeStr(buffer bytes.Buffer, str string) error {
	err := binary.Write(&buffer, binary.BigEndian, int64(len(str)))
	if err != nil {
		return err
	}
	_, err = buffer.WriteString(str)
	return err
}

func (gs *graphBinarySerializer) buildMessage(request *request, mimeLen byte, mimeType string) ([]byte, error) {
	buffer := bytes.Buffer{}

	// mime header
	buffer.WriteByte(mimeLen)
	buffer.WriteString(mimeType)

	// Version
	buffer.WriteByte(versionByte)

	// Request uuid
	bigIntUUID := uuidToBigInt(request.requestID)
	lower := bigIntUUID.Uint64()
	upperBigInt := bigIntUUID.Rsh(&bigIntUUID, 64)
	upper := upperBigInt.Uint64()
	err := binary.Write(&buffer, binary.BigEndian, upper)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buffer, binary.BigEndian, lower)
	if err != nil {
		return nil, err
	}

	// op
	err = binary.Write(&buffer, binary.BigEndian, uint32(len(request.op)))
	if err != nil {
		return nil, err
	}

	_, err = buffer.WriteString(request.op)
	if err != nil {
		return nil, err
	}

	// processor
	err = binary.Write(&buffer, binary.BigEndian, uint32(len(request.processor)))
	if err != nil {
		return nil, err
	}

	_, err = buffer.WriteString(request.processor)
	if err != nil {
		return nil, err
	}

	// args
	err = binary.Write(&buffer, binary.BigEndian, uint32(len(request.args)))
	for k, v := range request.args {
		_, err = gs.writerClass.writeValue(k, &buffer, true)
		if err != nil {
			return nil, err
		}

		_, err = gs.writerClass.writeValue(v, &buffer, true)
		if err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

func uuidToBigInt(requestID uuid.UUID) big.Int {
	var bigInt big.Int
	bigInt.SetString(strings.Replace(requestID.String(), "-", "", 4), 16)
	return bigInt
}

func readUUID(buffer *bytes.Buffer) (uuid.UUID, error) {
	var nullable byte
	err := binary.Read(buffer, binary.LittleEndian, &nullable)
	if err != nil {
		return uuid.UUID{}, err
	}
	uuidBytes := make([]byte, 16)
	err = binary.Read(buffer, binary.LittleEndian, uuidBytes)
	return uuid.FromBytes(uuidBytes)
}

func readMap(buffer *bytes.Buffer, gs *graphBinarySerializer) (map[string]interface{}, error) {
	var mapSize uint32
	err := binary.Read(buffer, binary.BigEndian, &mapSize)
	if err != nil {
		return nil, err
	}
	var mapData = map[string]interface{}{}
	for i := uint32(0); i < mapSize; i++ {
		var keyType DataType
		err = binary.Read(buffer, binary.BigEndian, &keyType)
		if err != nil {
			return nil, err
		} else if keyType != StringType {
			return nil, errors.New("expected string key for map")
		}
		var nullable byte
		err = binary.Read(buffer, binary.BigEndian, &nullable)
		if nullable != 0 {
			return nil, errors.New("expected non-null key for map")
		}

		k, err := readString(buffer)
		if err != nil {
			return nil, err
		}
		mapData[k], err = gs.readerClass.read(buffer)
		if err != nil {
			return nil, err
		}
	}
	return mapData, nil
}

func readString(buffer *bytes.Buffer) (string, error) {
	var strLength uint32
	err := binary.Read(buffer, binary.BigEndian, &strLength)
	if err != nil {
		return "", err
	}

	strBytes := make([]byte, strLength)
	err = binary.Read(buffer, binary.BigEndian, strBytes)
	if err != nil {
		return "", err
	}
	return string(strBytes[:]), nil
}

// deserializeMessage deserializes a response message
func (gs graphBinarySerializer) deserializeMessage(responseMessage []byte) (response, error) {
	var msg response
	buffer := bytes.Buffer{}
	buffer.Write(responseMessage)

	// Version
	_, err := buffer.ReadByte()
	if err != nil {
		return msg, err
	}

	// Response uuid
	msgUUID, err := readUUID(&buffer)
	if err != nil {
		return msg, err
	}

	// Status Code
	var statusCode uint32
	err = binary.Read(&buffer, binary.BigEndian, &statusCode)
	if err != nil {
		return msg, err
	}
	statusCode = statusCode & 0xFF

	// Nullable Status message
	var statusMessageNull byte
	var statusMessage string
	err = binary.Read(&buffer, binary.LittleEndian, &statusMessageNull)
	if statusMessageNull == 0 {
		statusMessage, err = readString(&buffer)
		if err != nil {
			return msg, err
		}
	}

	// Status Attributes
	statusAttributes, err := readMap(&buffer, &gs)
	if err != nil {
		return msg, err
	}

	// Meta Attributes
	metaAttributes, err := readMap(&buffer, &gs)
	if err != nil {
		return msg, err
	}

	// Result data
	data, err := gs.readerClass.read(&buffer)
	if err != nil {
		return msg, err
	}

	msg.responseID = msgUUID
	msg.responseStatus.code = uint16(statusCode)
	msg.responseStatus.message = statusMessage
	msg.responseStatus.attributes = statusAttributes
	msg.responseResult.meta = metaAttributes
	msg.responseResult.data = data

	return msg, nil
}

// private function for deserializing a request message for testing purposes
func (gs *graphBinarySerializer) deserializeRequestMessage(requestMessage *[]byte) (request, error) {
	buffer := bytes.Buffer{}
	var msg request
	buffer.Write(*requestMessage)
	// skip headers
	buffer.Next(33)
	// version
	_, err := buffer.ReadByte()
	if err != nil {
		return msg, err
	}
	msgUUID, err := gs.readerClass.readValue(&buffer, byte(UUIDType), false)
	if err != nil {
		return msg, err
	}
	msgOp, err := gs.readerClass.readValue(&buffer, byte(StringType), false)
	if err != nil {
		return msg, err
	}
	msgProc, err := gs.readerClass.readValue(&buffer, byte(StringType), false)
	if err != nil {
		return msg, err
	}
	msgArgs, err := gs.readerClass.readValue(&buffer, byte(MapType), false)
	if err != nil {
		return msg, err
	}

	msg.requestID = msgUUID.(uuid.UUID)
	msg.op = msgOp.(string)
	msg.processor = msgProc.(string)
	msg.args = msgArgs.(map[string]interface{})

	return msg, nil
}

// private function for serializing a response message for testing purposes
func (gs *graphBinarySerializer) serializeResponseMessage(response *response) ([]byte, error) {
	buffer := bytes.Buffer{}

	// version
	buffer.WriteByte(versionByte)

	// requestID
	_, err := gs.writerClass.writeValue(response.responseID, &buffer, true)
	if err != nil {
		return nil, err
	}
	// Status Code
	_, err = gs.writerClass.writeValue(response.responseStatus.code, &buffer, false)
	if err != nil {
		return nil, err
	}
	// Status message
	_, err = gs.writerClass.writeValue(response.responseStatus.message, &buffer, true)
	if err != nil {
		return nil, err
	}
	// Status attributes
	_, err = gs.writerClass.writeValue(response.responseStatus.attributes, &buffer, false)
	if err != nil {
		return nil, err
	}
	// Result meta
	_, err = gs.writerClass.writeValue(response.responseResult.meta, &buffer, false)
	if err != nil {
		return nil, err
	}
	// Result
	_, err = gs.writerClass.write(response.responseResult.data, &buffer)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
