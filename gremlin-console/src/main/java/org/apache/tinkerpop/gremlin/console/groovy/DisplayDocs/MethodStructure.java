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
package org.apache.tinkerpop.gremlin.console.groovy.DisplayDocs;

/**
 *
 * @author xristosoik (https://github.com/xristosoik)
 */
public class MethodStructure {

    private String methodName;
    private String documentation;
    private static int counter = 0;

    public MethodStructure(String name, String Block) {
        methodName = name;
        documentation = Block;
    }

    /**
     *
     * @param name The name of the Method in html code
     * @return The name of the method without
     */
    public static String cleanName(String name) {
        name = name.split("<h4>")[1];
        name = name.split("</h4>")[0];
        return (name);
    }

    /**
     *
     * @return Returns the method name
     */
    public String getMethodName() {
        return (methodName);
    }

    /**
     *
     * @return Return the method documentation
     */
    public String getMethodDoc() {
        return (documentation);
    }

    /**
     * This method has as a purpose to create a separator based on the length of
     * the method name.
     *
     * @param name the name of a method
     * @return the separator
     */
    public String createNameSeparator(String name) {
        String sep = "";
        int nameLength;
        /*
         I am identify the length of the method name
         */
        nameLength = name.toCharArray().length;

        for (int i = 0; i < nameLength; i++) {
            sep += "=";
        }
        sep += "\n";
        return (sep);
    }

    @Override
    public String toString() {
        String finalText = "", separator;
        separator = createNameSeparator(methodName.split("-")[0]);
        finalText += separator;
        finalText += methodName.split("-")[0] + "\n";
        finalText += separator;
        finalText += documentation + "\n";
        return (finalText);
    }
}
