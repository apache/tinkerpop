/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.kerby.kerberos.kerb.integration.test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Transport {

    public static class Acceptor {
        ServerSocket serverSocket;

        public Acceptor(int listenPort) throws IOException {
            this.serverSocket = new ServerSocket(listenPort);
        }

        public Connection accept() {
            try {
                Socket socket = serverSocket.accept();
                return new Connection(socket);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        public void close() {
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Connector {
        public static Connection connect(String host,
                                         int port) throws IOException {
            Socket socket = new Socket(host, port);
            return new Connection(socket);
        }
    }

    public static class Connection {
        private Socket socket;
        private DataInputStream instream;
        private DataOutputStream outstream;

        public Connection(Socket socket) throws IOException {
            this.socket = socket;
            instream = new DataInputStream(socket.getInputStream());
            outstream = new DataOutputStream(socket.getOutputStream());
        }

        public void close() throws IOException {
            socket.close();
        }

        public void sendToken(byte[] token) throws IOException {
            if (token != null) {
                outstream.writeInt(token.length);
                outstream.write(token);
            } else {
                outstream.writeInt(0);
            }
            outstream.flush();
        }

        public void sendMessage(Message msg) throws IOException {
            if (msg != null) {
                sendToken(msg.header);
                sendToken(msg.body);
            }
        }

        public void sendMessage(byte[] header, byte[] body) throws IOException {
            sendMessage(new Message(header, body));
        }

        public void sendMessage(String header, byte[] body) throws IOException {
            sendMessage(new Message(header, body));
        }

        public byte[] recvToken() throws IOException {
            int len = instream.readInt();
            if (len > 0) {
                byte[] token = new byte[len];
                instream.readFully(token);
                return token;
            }
            return null;
        }

        public Message recvMessage() throws IOException {
            byte[] header = recvToken();
            byte[] body = recvToken();
            Message msg = new Message(header, body);
            return msg;
        }
    }

    public static class Message {
        public byte[] header;
        public byte[] body;


        Message(byte[] header, byte[] body) {
            this.header = header;
            this.body = body;
        }

        public Message(String header, byte[] body) {
            this.header = header.getBytes(StandardCharsets.UTF_8);
            this.body = body;
        }
    }
}
