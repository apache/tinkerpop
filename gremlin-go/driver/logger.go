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
	"encoding/json"
	"log"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
)

// LogVerbosity is an alias for valid logging verbosity levels.
type LogVerbosity int

const (
	// Debug verbosity will log everything, including fine details.
	Debug LogVerbosity = iota
	// Info verbosity will log messages up to standard procedure flow.
	Info
	// Warning verbosity will log messages up to warnings.
	Warning
	// Error verbosity level log only error messages.
	Error
	// Off verbosity level disables logging.
	Off
)

// Logger is the interface required to be implemented for use with gremlingo.
type Logger interface {
	Log(verbosity LogVerbosity, v ...interface{})
	Logf(verbosity LogVerbosity, format string, v ...interface{})
}

type defaultLogger struct {
}

func (logger *defaultLogger) Log(verbosity LogVerbosity, v ...interface{}) {
	log.Print(v...)
}

func (logger *defaultLogger) Logf(verbosity LogVerbosity, format string, v ...interface{}) {
	log.Printf(format, v...)
}

type logHandler struct {
	logger    Logger
	verbosity LogVerbosity
	localizer *i18n.Localizer
}

func newLogHandler(logger Logger, verbosity LogVerbosity, locale language.Tag) *logHandler {
	bundle := i18n.NewBundle(language.English)
	bundle.RegisterUnmarshalFunc("json", json.Unmarshal)

	// Register resource package here for additional languages.
	bundle.LoadMessageFile("resources/en.json")
	localizer := i18n.NewLocalizer(bundle, locale.String())
	return &logHandler{logger, verbosity, localizer}
}

func (logHandler *logHandler) log(verbosity LogVerbosity, errorKey errorKey) {
	logHandler.logf(verbosity, errorKey)
}

func (logHandler *logHandler) logf(verbosity LogVerbosity, errorKey errorKey, v ...interface{}) {
	if verbosity >= logHandler.verbosity {
		config := i18n.LocalizeConfig{
			MessageID: string(errorKey),
		}
		localizedMessage, _ := logHandler.localizer.Localize(&config)
		logHandler.logger.Logf(verbosity, localizedMessage, v...)
	}
}

type errorKey string

const (
	readFailed               errorKey = "READ_FAILED"
	writeFailed              errorKey = "WRITE_FAILED"
	serializeDataTypeError   errorKey = "UNKNOWN_SER_DATATYPE"
	deserializeDataTypeError errorKey = "UNKNOWN_DESER_DATATYPE"
	nullInput                errorKey = "NULL_INPUT"
	unmatchedDataType        errorKey = "UNMATCHED_DATATYPE"
	unexpectedNull           errorKey = "UNEXPECTED_NULL_VALUE"
	notMap                   errorKey = "NOT_MAP_TYPE"
	malformedURL             errorKey = "MALFORMED_URL"
	transportCloseFailed     errorKey = "TRANSPORT_CLOSE_FAILED"
	notSlice                 errorKey = "NOT_SLICE_TYPE"
	closeConnection          errorKey = "CLOSING_CONNECTION"
	connectConnection        errorKey = "OPENING_CONNECTION"
	failedConnection         errorKey = "FAILED_CONNECTION"
	writeRequest             errorKey = "WRITE_REQUEST"
	readLoopError            errorKey = "READ_LOOP_ERROR"
	errorCallback            errorKey = "ERROR_CALLBACK"
	creatingRequest          errorKey = "CREATING_REQUEST"
	readComplete             errorKey = "READ_COMPLETE"
)
