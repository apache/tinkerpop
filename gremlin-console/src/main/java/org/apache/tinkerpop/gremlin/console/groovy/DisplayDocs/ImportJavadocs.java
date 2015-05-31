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
         * @return true if this class exists and false if not.
         */
	public static boolean findClass (String className) {
		try {
			currentDoc = new DocStructure(findPath2TheJavadoc(className), className);
		} catch (Exception e) {
                        System.out.println("sgbddbb");
			return(false);
		}
		return(true);
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
		try {
			File javadocDirectory = new File ("target/apache-gremlin-console-3.0.0-SNAPSHOT-standalone/javadocs");
                        String[] folders = javadocDirectory.list();
			for (String dir : folders) {
				BufferedReader in = new BufferedReader(
                                        new FileReader("target/apache-gremlin-console-3.0.0-SNAPSHOT-standalone/javadocs/" + dir + 
                                                "/allclasses-noframe.html"));
				String str;
				while ((str = in.readLine()) != null) {
					find = str.contains(className);
					if (find) {
						line = str;
						break;
					}
				}
                                path[0] = dir;
				in.close();
				if (find) {
					break;
				}
			}
		} catch (Exception e) {
			System.out.println("xhbdxzfhn");
		}
                path[1] = line;
		return(path);
	}

}