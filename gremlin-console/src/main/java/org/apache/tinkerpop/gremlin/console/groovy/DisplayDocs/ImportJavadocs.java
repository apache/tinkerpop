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
import java.net.URL;
import java.net.URLConnection;

/**
 *
 * @author xristosoik (https://github.com/xristosoik)
 */
public class ImportJavadocs {

    static protected DocStructure currentDoc;
    static private boolean webpage = false;

    /**
     * This method find the target fill to the javadocs of the Class that is
     * specifies by the className.
     *
     * @param className The name of a class whom javadocs we want to import.
     * @return The DocStructure of the class.
     */
    public static DocStructure findClass(String className) {
        try {
            className = className.replaceAll("\"", "");
            String path = findPath2TheJavadoc(">" + className + "<");
            DocStructure tempDoc = new DocStructure(path, className);
            currentDoc = tempDoc;
        } catch (Exception e) {
            System.out.println("Unsuccesful import!");
        } finally {
            return (currentDoc);
        }
    }

    /**
     * Clean up the line keeping only the path.
     *
     * @param className
     * @return
     */
    protected static String findPath2TheJavadoc(String className) {
        String path[] = findPath2CorrectLine(className);
        path[1] = path[1].split("href=\"")[1];
        path[1] = path[1].split("\" title")[0];
        if (!webpage) {
            return (path[0] + File.separator + path[1]);
        } else {
            return (path[0] + "/" + path[1]);
        }
    }

    /**
     * Τhis method search all directories for the correct line of html.
     *
     * @param className
     * @return correct line html free.
     */
    protected static String[] findPath2CorrectLine(String className) {
        boolean find = false;
        String line = "";
        String path[] = new String[2];
        path[0] = ".";
        path[1] = "";
        int i = 0;
        try {
            webpage = false;
            path[0] = findCorrectPath2Javadocs();
            if (path[0].equals("")) {
                throw new Exception();
            }
            String[] folders = {"/core", "/full"};
            for (String dir : folders) {
                BufferedReader in = new BufferedReader(
                        new FileReader(path[0] + dir
                                + "/allclasses-noframe.html"));
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
            path[1] = line;
        } catch (Exception e) {
            System.out.println("ATTENTION!!! It was not able to access the local javadoc folder");
            path = findPath2CorrectLineWeb(className);
        } finally {
            return (path);
        }
    }

    /**
     * Search all the possible paths to identify which is the available one.
     *
     * @return
     */
    public static String findCorrectPath2Javadocs() {
        String path;
        try {
            File pathDefinition = new File(".");

            path = pathDefinition.getCanonicalPath().replace("bin", "javadocs");
            File javadocDirectory = new File(path);
            if (javadocDirectory.list() != null) {
                return (path);
            }
        } catch (Exception e) {
            return ("");
        }
        return ("");
    }

    /**
     * Find the specific url to the class javadoc.
     *
     * @param className The name the class.
     * @return a string array of the 2 parts of the specific url
     */
    protected static String[] findPath2CorrectLineWeb(String className) {
        boolean find = false;
        String line = "";
        String path[] = new String[2];
        path[0] = ".";
        int i = 0;
        try {
            System.out.println("We will search for javadocs online!!!!");
            path[0] = findCorrectURL();
            String[] folders = {"/core", "/full"};
            URL tinkerpop;
            URLConnection connection;
            for (String dir : folders) {
                tinkerpop = new URL(path[0] + dir
                        + "/allclasses-noframe.html");
                connection = tinkerpop.openConnection();
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(
                                connection.getInputStream(), "UTF-8"));
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
                    webpage = true;
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        path[1] = line;
        return (path);
    }

    /**
     * Search for the current url to the javadocs
     *
     * @return the correct url
     */
    public static String findCorrectURL() {
        String path = "";
        try {
            URL tinkerpop = new URL("http://tinkerpop.incubator.apache.org/");
            URLConnection connection = tinkerpop.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream(), "UTF-8"));
            String line;
            while ((line = in.readLine()) != null) {
                if (line.contains("http://tinkerpop.incubator.apache.org/javadocs/")) {
                    path = line;
                    path = path.split("href=\"")[1];
                    path = path.split("/\">")[0];
                    break;
                }
            }
            in.close();
            path = path.split("/core")[0];
            path = path.split("/full")[0];
            return (path);
        } catch (Exception e) {

        }
        return ("");
    }

    public static Boolean getWebpage() {
        return (webpage);
    }
}
