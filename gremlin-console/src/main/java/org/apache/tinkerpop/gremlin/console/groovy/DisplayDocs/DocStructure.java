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
 * @author  xristosoik (https://github.com/xristosoik)
 */
import java.util.TreeMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
public class DocStructure {

	private String className;
	private String classPath;
	private TreeMap<String, MethodStructure> methodList;
        private final String methodSeparator = "<!-- ============ METHOD DETAIL ========== -->";

	public DocStructure (String path, String name) throws Exception {
		className = name;
		classPath = path;
                loadMethodsInfo(path);
	}

	protected void loadMethodsInfo (String path) {
                methodList = new TreeMap<String, MethodStructure>();
		boolean find, finishReading = false;
                String temp = "", name;
                MethodStructure temporary;
                int position = 0, counter = 0;
		try {
			BufferedReader in = new BufferedReader(new FileReader("target/apache-gremlin-console-3.0.0-SNAPSHOT-standalone/javadocs/" + path));
			String str;
                        while ((str = in.readLine()) != null) {
                            if (finishReading)
                                break;
                            find = str.contains(methodSeparator);
                            if (find) {
                                while ((str = in.readLine()) != null) {
                                    if (position == 0 && str.contains("<!--   -->")) {
                                        position++;
                                        continue;
                                    }
                                    if (position == 1) {
                                        if (str.contains("</a>")) {
                                            position++;
                                            continue;
                                        } else {
                                            position = 0;
                                        }
                                    }
                                    if (position == 2) {
                                        if (str.contains("<ul class=\"blockList\">")) {
                                            position++;
                                            continue;
                                        } else if (str.contains("<ul class=\"blockListLast\">")) {
                                            position++;
                                            finishReading = true;
                                            continue;
                                        } else {
                                            position = 0;
                                        }
                                    }
                                    if (position == 3) {
                                        if (str.contains("<li class=\"blockList\">")) {
                                            position++;
                                            continue;
                                        } else {
                                            position = 0;
                                        }
                                    }
                                    if (position == 4 && str.contains("<h4>")) {
                                        name = str;
                                        while ((str = in.readLine()) != null) {
                                            if (str.contains("</ul>")) {
                                                position = 0;
                                                break;
                                            }
                                            temp += str;
                                        }
                                        name = MethodStructure.cleanName(name);
                                        if (methodList.containsKey(name)) {
                                            name += counter;
                                            counter++;
                                        } else
                                            counter = 0;
                                        temporary = new MethodStructure(name, temp);
                                        methodList.put(temporary.getMethodName(), temporary);
                                    } else {
                                        position =0;
                                    }
                                }
                            }
                        }
		} catch (Exception e) {
                   System.out.println(e.getMessage()); 
                }

	}
        
        public String getClassName() {
            return(className);
        }
        
        public String getClassPath() {
            return(classPath);
        }
        
        public TreeMap<String, MethodStructure> getMethodList() {
            return(methodList);
        }
}
