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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.util.Gremlin;
import javax.naming.NamingException;

public class UserAgent {

    /**
     * Request header name for user agent
     */
    public static final String USER_AGENT_HEADER_NAME = "User-Agent";
    /**
     * User Agent body to be sent in web socket handshake
     * Has the form of:
     * [Application Name] [GLV Name]/[Version] [Language Runtime Version] [OS]/[Version] [CPU Architecture]
     */
    public static final String USER_AGENT;

    static {
        String applicationName = "";
        try {
            applicationName = ((String)(new javax.naming.InitialContext().lookup("java:app/AppName"))).replace(' ', '_');
        } catch (NamingException e) {
            applicationName = "NotAvailable";
        };

        final String glvVersion = Gremlin.version().replace(' ', '_');
        final String javaVersion = System.getProperty("java.version", "NotAvailable").replace(' ', '_');
        final String osName = System.getProperty("os.name", "NotAvailable").replace(' ', '_');
        final String osVersion = System.getProperty("os.version", "NotAvailable").replace(' ', '_');
        final String cpuArch = System.getProperty("os.arch", "NotAvailable").replace(' ', '_');

        USER_AGENT =  String.format("%s Gremlin-Java.%s %s %s.%s %s",
                                            applicationName, glvVersion, javaVersion,
                                            osName, osVersion, cpuArch);
    }
}
