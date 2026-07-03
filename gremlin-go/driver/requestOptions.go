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

type RequestOptions struct {
	timeoutMs     int
	batchSize             int
	userAgent             string
	parametersString      string
	materializeProperties string
	bulkResults           *bool
	transactionId         string
}

type RequestOptionsBuilder struct {
	timeoutMs     int
	batchSize             int
	userAgent             string
	parameters            map[string]interface{}
	parametersString      string
	materializeProperties string
	bulkResults           *bool
	transactionId         string
}

func (builder *RequestOptionsBuilder) SetTimeoutMs(timeoutMs int) *RequestOptionsBuilder {
	builder.timeoutMs = timeoutMs
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

func (builder *RequestOptionsBuilder) SetParameters(parameters map[string]interface{}) *RequestOptionsBuilder {
	if builder.parametersString != "" {
		panic("cannot mix SetParameters() with SetParametersString()")
	}
	builder.parameters = parameters
	return builder
}

func (builder *RequestOptionsBuilder) SetParametersString(parametersString string) *RequestOptionsBuilder {
	if builder.parameters != nil {
		panic("cannot mix SetParametersString() with SetParameters()")
	}
	builder.parametersString = parametersString
	return builder
}

func (builder *RequestOptionsBuilder) SetMaterializeProperties(materializeProperties string) *RequestOptionsBuilder {
	builder.materializeProperties = materializeProperties
	return builder
}

func (builder *RequestOptionsBuilder) SetBulkResults(bulkResults bool) *RequestOptionsBuilder {
	builder.bulkResults = &bulkResults
	return builder
}

func (builder *RequestOptionsBuilder) SetTransactionId(transactionId string) *RequestOptionsBuilder {
	builder.transactionId = transactionId
	return builder
}

func (builder *RequestOptionsBuilder) AddParameter(key string, parameter interface{}) *RequestOptionsBuilder {
	if builder.parametersString != "" {
		panic("cannot mix AddParameter() with SetParametersString()")
	}
	if builder.parameters == nil {
		builder.parameters = make(map[string]interface{})
	}
	builder.parameters[key] = parameter
	return builder
}

func (builder *RequestOptionsBuilder) Create() RequestOptions {
	requestOptions := new(RequestOptions)

	requestOptions.timeoutMs = builder.timeoutMs
	requestOptions.batchSize = builder.batchSize
	requestOptions.userAgent = builder.userAgent
	requestOptions.materializeProperties = builder.materializeProperties
	requestOptions.bulkResults = builder.bulkResults
	requestOptions.transactionId = builder.transactionId

	// convert map parameters to string at creation time, matching Java's RequestOptions.Builder.create()
	if builder.parametersString != "" {
		requestOptions.parametersString = builder.parametersString
	} else if builder.parameters != nil {
		requestOptions.parametersString = ConvertParametersToString(builder.parameters)
	}

	return *requestOptions
}
