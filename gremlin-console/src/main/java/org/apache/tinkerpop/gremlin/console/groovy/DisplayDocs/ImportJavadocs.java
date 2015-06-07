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

import java.io.*;

/**
 *
 * @author xristosoik (https://github.com/xristosoik)
 */
public class ImportJavadocs {
    
	static protected DocStructure currentDoc;

	/*public static void main(String[] args) {
            System.out.println(findClass(">" + "Client" + "<"));
            System.out.println(currentDoc.getMethodList().size());
	}*/

	/**
         * This method find the target fill to the javadocs of the Class that is specifies by the className.
         * @param className The name of a class whom javadocs we want to import.
         * @return The DocStructure of the class.
         */
	public static DocStructure findClass (String className) {
		try {
                        className = className.replaceAll("\"", "");
                        String path = findPath2TheJavadoc(">" + className + "<");
			DocStructure tempDoc = new DocStructure(path, className);
                        currentDoc = tempDoc;
		} catch (Exception e) {
                        System.out.println("Unsuccesful import!");
		}
                return(currentDoc);
	}
        
        /**
         *  Clean up the line keeping only the path.
         * @param className
         * @return 
         */
	protected static String findPath2TheJavadoc (String className) {
		String path[] = findPath2CorrectLine(className);
                path[1] = path[1].split("href=\"")[1];
		path[1] = path[1].split("\" title")[0];
		return(path[0] + "/" + path[1]);
	}

        /**
         * Τhis method search all directories for the correct line of html.
         * @param className
         * @return correct line html free.
         */
	protected static String[] findPath2CorrectLine (String className) {
		boolean find = false;
		String line = "";
                String path[] = new String[2];
                path[0] = ".";
                int i = 0;
		try {
                        File pathDefinition = new File(".");
                        path[0] = pathDefinition.getCanonicalPath().replace("bin", "target/site/apidocs");
			File javadocDirectory = new File (path[0]);
                        String[] folders = {"/core", "/full"};
			for (String dir : folders) {
				BufferedReader in = new BufferedReader(
                                        new FileReader(path[0] + dir + 
                                                "/allclasses-noframe.html"));
                                String str;
				while ((str = in.readLine()) != null) {
					find = str.contains(className);
					if (find) {
						line = str;
						break;
					}
				}
				in.close();
				if (find) {
                                    path[0] += dir;
                                    break;
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
                path[1] = line;
		return(path);
	}
}