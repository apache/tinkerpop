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
	"compress/zlib"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

const keepAliveIntervalDefault = 5 * time.Second
const writeDeadlineDefault = 3 * time.Second
const writeChannelSizeDefault = 100
const connectionTimeoutDefault = 5 * time.Second

type HttpTransporter struct {
	url             string
	isClosed        bool
	connSettings    *connectionSettings
	responseChannel chan []byte
	httpClient      *http.Client
	wg              *sync.WaitGroup
}

func NewHttpTransporter(url string, connSettings *connectionSettings, httpClient *http.Client) *HttpTransporter {
	wg := &sync.WaitGroup{}

	return &HttpTransporter{
		url:             url,
		connSettings:    connSettings,
		responseChannel: make(chan []byte, writeChannelSizeDefault),
		httpClient:      httpClient,
		wg:              wg,
	}
}

func (transporter *HttpTransporter) Write(data []byte) error {
	fmt.Println("Sending request message")
	u, err := url.Parse(transporter.url)
	if err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return err
	}

	header := http.Header{
		"content-type": {graphBinaryMimeType},
		"host":         {host},
		"accept":       {graphBinaryMimeType},
	}
	if transporter.connSettings.enableUserAgentOnConnect {
		header.Set(userAgentHeader, userAgent)
	}
	if transporter.connSettings.enableCompression {
		header.Set("accept-encoding", "deflate")
	}

	body := io.NopCloser(bytes.NewReader(data))
	req := http.Request{
		Method:        "POST",
		URL:           u,
		Header:        header,
		Body:          body,
		ContentLength: int64(len(data)),
	}

	resp, err := transporter.httpClient.Do(&req)
	if err != nil {
		return err
	}

	reader := resp.Body
	if resp.Header.Get("content-encoding") == "deflate" {
		reader, err = zlib.NewReader(resp.Body)
		if err != nil {
			return err
		}
	}

	// TODO handle chunked encoding

	all, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	str := hex.EncodeToString(all)
	_, _ = fmt.Fprintf(os.Stdout, "Received response data : %s\n", str)

	fmt.Println("Sending response to responseChannel")
	transporter.responseChannel <- all
	return nil
}

func (transporter *HttpTransporter) Read() ([]byte, error) {
	fmt.Println("Reading from responseChannel")
	msg, ok := <-transporter.responseChannel
	if !ok {
		return []byte{}, errors.New("failed to read from channel")
	}
	return msg, nil
}

func (transporter *HttpTransporter) Close() (err error) {
	fmt.Println("Closing http transporter")
	if !transporter.isClosed {
		if transporter.responseChannel != nil {
			close(transporter.responseChannel)
		}
		transporter.isClosed = true
	}
	return
}
