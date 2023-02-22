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
	"github.com/google/uuid"
)

type RequestOptions struct {
	requestID         uuid.UUID
	evaluationTimeout int
	batchSize         int
	userAgent         string
	bindings          map[string]interface{}
}

type RequestOptionsBuilder struct {
	requestID         uuid.UUID
	evaluationTimeout int
	batchSize         int
	userAgent         string
	bindings          map[string]interface{}
}

func (builder *RequestOptionsBuilder) SetRequestId(requestId uuid.UUID) *RequestOptionsBuilder {
	builder.requestID = requestId
	return builder
}

func (builder *RequestOptionsBuilder) SetEvaluationTimeout(evaluationTimeout int) *RequestOptionsBuilder {
	builder.evaluationTimeout = evaluationTimeout
	return builder
}

func (builder *RequestOptionsBuilder) SetBatchSize(batchSize int) *RequestOptionsBuilder {
	builder.batchSize = batchSize
	return builder
}

func (builder *RequestOptionsBuilder) SetUserAgent(userAgent string) *RequestOptionsBuilder {
	builder.userAgent = userAgent
	return builder
}

func (builder *RequestOptionsBuilder) SetBindings(bindings map[string]interface{}) *RequestOptionsBuilder {
	builder.bindings = bindings
	return builder
}

func (builder *RequestOptionsBuilder) AddBinding(key string, binding interface{}) *RequestOptionsBuilder {
	if builder.bindings == nil {
		builder.bindings = make(map[string]interface{})
	}
	builder.bindings[key] = binding
	return builder
}

func (builder *RequestOptionsBuilder) Create() RequestOptions {
	requestOptions := new(RequestOptions)

	requestOptions.requestID = builder.requestID
	requestOptions.evaluationTimeout = builder.evaluationTimeout
	requestOptions.batchSize = builder.batchSize
	requestOptions.userAgent = builder.userAgent
	requestOptions.bindings = builder.bindings

	return *requestOptions
}
