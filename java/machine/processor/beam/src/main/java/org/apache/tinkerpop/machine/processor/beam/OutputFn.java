/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.machine.processor.beam;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class OutputFn<C, S> extends DoFn<Traverser<C, S>, Void> {

    private final String traverserServerLocation;
    private final int traverserServerPort;
    private Socket traverserServerSocket;
    private ObjectOutputStream outputStream;

    OutputFn(final String traverserServerLocation, final int traverserServerPort) {
        this.traverserServerLocation = traverserServerLocation;
        this.traverserServerPort = traverserServerPort;
    }

    @ProcessElement
    public void processElement(final @Element Traverser<C, S> traverser) {
        try {
            this.outputStream.writeObject(traverser);
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @StartBundle
    public void startBundle() {
        // only create a connection if results are generated at this branch
        if (null == this.traverserServerSocket) {
            try {
                this.traverserServerSocket = new Socket(this.traverserServerLocation, this.traverserServerPort);
                this.outputStream = new ObjectOutputStream(this.traverserServerSocket.getOutputStream());
            } catch (final Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    @FinishBundle
    public void finishBundle() {
        try {
            this.outputStream.flush();
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Teardown
    public void stop() {
        if (null != this.traverserServerSocket) {
            try {
                this.outputStream.flush();
                this.outputStream.close();
                this.traverserServerSocket.close();
            } catch (final Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    /*@Override
    public String toString() {
        return "";
    }*/
}