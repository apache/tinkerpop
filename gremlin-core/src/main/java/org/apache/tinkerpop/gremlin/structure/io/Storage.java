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

package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * Storage is a standard API that providers can implement to allow abstract UNIX-like file system for data sources.
 * The methods provided by Storage are similar in form and behavior to standard Linux operating system commands.
 *<ul>
 * <li>A <b>name pattern</b> (file or directory) is a sequence of characters, not containing "/", leading spaces, trailing spaces. </li>
 * <li>A <b>name</b> (file or directory name) is a name pattern, not containing "*" or "?".</li>
 * <li>A <b>pattern</b> is a sequence of names separated with "/", optionally ending at a name pattern.
 * <pre>
 *   &lt;pattern&gt; ::= &lt;absolute pattern&gt; |
 *                 &lt;relative pattern&gt;
 *   &lt;absolute path&gt; ::= / [&lt;relative pattern&gt;]
 *   &lt;relative path&gt; ::= &lt;name&gt; {/ &lt;name&gt;} [/ &lt;name pattern&gt;] [/] |
 *                       &lt;name pattern&gt; [/]
 * </pre></li>
 * <li>A <b>path</b> is a path is a pattern, not containing any name pattern. </li>
 * </ul>
 * NOTE: <ol>
 * <li>Even though the syntax allows patterns with trailing "/", they are treated as referring the same
 *    file or directory as the path without the trailing /
 * <li>This is an abstract file system abstracting the underlying physical file system if any. Thus, under Windows the
 *     directories separator is still /, no matter that Windows uses \
 * </ol>
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Storage {
    /**
	 * The file and directory names separator in this uniform UNIX-like abstract file system
	 */
    String FILE_SEPARATOR = "/";

    String ROOT_DIRECTORY = FILE_SEPARATOR;

    /**
     * List all the data sources in the root directory.
     *
     * @return non-null list of files (data sources) and directories in the root directory (/)
     * @see #ls(String)
     */
    public List<String> ls();

    /**
     * List all the files (e.g. data sources) and directories matching the location pattern.
     *
     * @param pattern non-null pattern specifying a set of files and directories. Cases:<ul>
     *    <li>a path to a file - specifies a single file to list
     *    <li>a path to a directory - specifies all files and directories immediately nested in that directory to list
     *    <li>pattern - specifies a set of files and directories to list
     *    <li>/ - specifies the root directory to list its contents
     *  </ul>
     * @return non-null list of files (data sources) and directories matching the pattern.
     */
    public List<String> ls(final String pattern);

    /**
     * Recursively copy all the data sources from the source location to the target location.
     *
     * @param sourcePattern non-null pattern specifying a set of files and directories. Cases:<ul>
     *    <li>a path to a file - specifies a single file
     *    <li>a path to a directory - specifies all files and directories nested (recursively) in that directory
     *    <li>pattern - specifies a set of files and directories
     *    <li>/ - specifies the contents of the root directory (recursively)
     *  </ul>
     * @param targetDirectory non-null directory where to copy to
     * @return whether data sources were copied
     */
    public boolean cp(final String sourcePattern, final String targetDirectory);

    /**
     * Determine whether the specified location has a data source.
     *
     * @param pattern non-null pattern specifying a set of files and directories. Examples:<ul>
     *    <li>a path to a file - specifies a single file
     *    <li>a path to a directory - specifies the contents of that directory as all files and directories immediately nested in it
     *    <li>pattern - specifies a set of files and directories
     *    <li>/ - specifies the immediate contents of the root directory
     *  </ul>
     *
     * @return true if the pattern specifies a non-empty set of files and directories
     */
    public boolean exists(final String pattern);

    /**
     * Recursively remove the file (data source) at the specified location.
     *
     * NOTE: Some implementations derive the notion of the containing directory from the presence of the file,
     *       so removing all files from a directory in those implementations removes also their directory.
     *
     * @param pattern non-null pattern specifying a set of files and directories. Examples:<ul>
     *    <li>a path to a file - specifies a single file
     *    <li>a path to a directory - specifies <b>that directory and</b> all files and directories recursively nested in it
     *    <li>pattern - specifies a set of files and directories
     *    <li>/ - specifies the root directory
     *  </ul>
     * @return true if all specified files and directories were removed
     */
    public boolean rm(final String pattern);

    /**
     * Get a string representation of the specified number of lines at the data source location.
     *
     * @param location the data source location
     * @return an iterator of lines
     */
    public default Iterator<String> head(final String location) {
        return this.head(location, Integer.MAX_VALUE);
    }

    /**
     * Get a string representation of the specified number of lines at the data source location.
     *
     * @param location   the data source location
     * @param totalLines the total number of lines to retrieve
     * @return an iterator of lines.
     */
    public Iterator<String> head(final String location, final int totalLines);

    /**
     * Get the vertices at the specified graph location.
     *
     * @param location    the location of the graph (or the root location and search will be made)
     * @param readerClass the class of the parser that understands the graph format
     * @param totalLines  the total number of lines of the graph to return
     * @return an iterator of vertices.
     */
    public Iterator<Vertex> head(final String location, final Class readerClass, final int totalLines);

    /**
     * Get the vertices at the specified graph location.
     *
     * @param location    the location of the graph (or the root location and search will be made)
     * @param readerClass the class of the parser that understands the graph format
     * @return an iterator of vertices.
     */
    public default Iterator<Vertex> head(final String location, final Class readerClass) {
        return this.head(location, readerClass, Integer.MAX_VALUE);
    }

    /**
     * Get the {@link KeyValue} data at the specified memory location.
     *
     * @param location    the root location of the data
     * @param memoryKey   the memory key
     * @param readerClass the class of the parser that understands the memory format
     * @param totalLines  the total number of key-values to return
     * @return an iterator of key-values.
     */
    public <K, V> Iterator<KeyValue<K, V>> head(final String location, final String memoryKey, final Class readerClass, final int totalLines);


    /**
     * Get the {@link KeyValue} data at the specified memory location.
     *
     * @param location    the root location of the data
     * @param memoryKey   the memory key
     * @param readerClass the class of the parser that understands the memory format
     * @return an iterator of key-values.
     */
    public default <K, V> Iterator<KeyValue<K, V>> head(final String location, final String memoryKey, final Class readerClass) {
        return this.head(location, memoryKey, readerClass, Integer.MAX_VALUE);
    }


    /**
     * @param path non-null local file path
     * @return non-null, not empty path in the {@link Storage} file system.
     */
    public static String toPath(final File path) {
        return toPath(path.getAbsolutePath());
    }

    /**
     * @param path non-null local file path
     * @return non-null, not empty path in the {@link Storage} file system.
     */
    public static String toPath(final String path) {
        return path.replace("\\", FILE_SEPARATOR);
    }
}
