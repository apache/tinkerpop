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
package org.apache.tinkerpop.gremlin.groovy.util

import groovy.grape.Grape
import org.apache.commons.lang3.SystemUtils
import org.apache.tinkerpop.gremlin.groovy.plugin.Artifact

import java.nio.file.*
import java.util.jar.JarFile
import java.util.jar.Manifest

/**
 * This class is a rough copy of the {@code InstallCommand} in Gremlin Console.  There are far more detailed
 * comments there with respect to the workings of this class.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class DependencyGrabber {

    private final static String fileSep = System.getProperty("file.separator")
    private final ClassLoader classLoaderToUse
    private final String extensionDirectory

    public DependencyGrabber(final ClassLoader cl, final String extensionDirectory) {
        this.classLoaderToUse = cl
        this.extensionDirectory = extensionDirectory
    }

    def void copyDependenciesToPath(final Artifact artifact) {
        final def dep = makeDepsMap(artifact)
        final String extClassPath = getPathFromDependency(dep)
        final String extLibPath = extClassPath + fileSep + "lib"
        final String extPluginPath = extClassPath + fileSep + "plugin"
        final File f = new File(extClassPath)

        if (f.exists()) throw new IllegalStateException("a module with the name ${dep.module} is already installed")
        if (!f.mkdirs()) throw new IOException("could not create directory at ${f}")
        if (!new File(extLibPath).mkdirs()) throw new IOException("could not create directory at ${extLibPath}")
        if (!new File(extPluginPath).mkdirs()) throw new IOException("could not create directory at ${extPluginPath}")

        new File(extClassPath + fileSep + "plugin-info.txt").withWriter { out -> out << [artifact.group, artifact.artifact, artifact.version].join(":") }

        def fs = FileSystems.default
        def targetPluginPath = fs.getPath(extPluginPath)
        def targetLibPath = fs.getPath(extLibPath)

        def filesAlreadyInPath = []
        def libClassPath
        try {
            libClassPath = fs.getPath(System.getProperty("user.dir") + fileSep + "lib")
            getFileNames(filesAlreadyInPath, libClassPath)
        } catch (Exception ignored) {
            println "Detected a non-standard Gremlin directory structure during install.  Expecting a 'lib' " +
                    "directory sibling to 'ext'. This message does not necessarily imply failure, however " +
                    "the console requires a certain directory structure for proper execution. Altering that " +
                    "structure can lead to unexpected behavior."
        }

        final def dependencyLocations = [] as Set<URI>
        dependencyLocations.addAll(Grape.resolve([classLoader: this.classLoaderToUse], null, dep))

        // get dependencies for the plugin path which should be part of the class path
        dependencyLocations.collect(convertUriToPath(fs))
                .findAll { !(it.fileName.toFile().name ==~ /(slf4j|logback\-classic)-.*\.jar/) }
                .findAll {!filesAlreadyInPath.collect { it.getFileName().toString() }.contains(it.fileName.toFile().name)}
                .each(copyTo(targetPluginPath))
        getAdditionalDependencies(targetPluginPath, artifact).collect(convertUriToPath(fs))
            .findAll { !(it.fileName.toFile().name ==~ /(slf4j|logback\-classic)-.*\.jar/) }
            .findAll { !filesAlreadyInPath.collect { it.getFileName().toString() }.contains(it.fileName.toFile().name)}
            .each(copyTo(targetPluginPath))

        // get dependencies for the lib path.  the lib path should not filter out any jars - used for reference
        dependencyLocations.collect(convertUriToPath(fs)).each(copyTo(targetLibPath))
        getAdditionalDependencies(targetLibPath, artifact).collect(convertUriToPath(fs)).each(copyTo(targetLibPath))

        alterPaths("Gremlin-Plugin-Paths", targetPluginPath, artifact)
        alterPaths("Gremlin-Lib-Paths", targetLibPath, artifact)
    }

    private static Closure copyTo(final Path path) {
        return { Path p ->
            def copying = path.resolve(p.fileName)
            Files.copy(p, copying, StandardCopyOption.REPLACE_EXISTING)
            println "Copying - $copying"
        }
    }

    /**
     * Windows places a starting forward slash in the URI that needs to be stripped off or else the
     * {@code FileSystem} won't properly resolve it.
     */
    private static Closure convertUriToPath(final FileSystem fs) {
        return { URI uri ->
            def p = SystemUtils.IS_OS_WINDOWS ? uri.path.substring(1) : uri.path
            return fs.getPath(p)
        }
    }

    private Set<URI> getAdditionalDependencies(final Path extPath, final Artifact artifact) {
        try {
            def pathToInstalled = extPath.resolve(artifact.artifact + "-" + artifact.version + ".jar")
            final JarFile jar = new JarFile(pathToInstalled.toFile())
            final Manifest manifest = jar.getManifest()
            def attrLine = manifest.mainAttributes.getValue("Gremlin-Plugin-Dependencies")
            def additionalDependencies = [] as Set<URI>
            if (attrLine != null) {
                def splitLine = attrLine.split(";")
                splitLine.each {
                    def artifactBits = it.split(":")
                    def additional = new Artifact(artifactBits[0], artifactBits[1], artifactBits[2])

                    final def additionalDep = makeDepsMap(additional)
                    additionalDependencies.addAll(Grape.resolve([classLoader: this.classLoaderToUse], null, additionalDep))
                }
            }

            return additionalDependencies
        } catch (Exception ex) {
            throw new RuntimeException(ex)
        }
    }

    private static alterPaths(final String manifestEntry, final Path extPath, final Artifact artifact) {
        try {
            def pathToInstalled = extPath.resolve(artifact.artifact + "-" + artifact.version + ".jar")
            final JarFile jar = new JarFile(pathToInstalled.toFile());
            final Manifest manifest = jar.getManifest()
            def attrLine = manifest.mainAttributes.getValue(manifestEntry)
            if (attrLine != null) {
                def splitLine = attrLine.split(";")
                splitLine.each {
                    if (it.endsWith("="))
                        Files.delete(extPath.resolve(it.substring(0, it.length() - 1)))
                    else {
                        def kv = it.split("=")
                        Files.move(extPath.resolve(kv[0]), extPath.resolve(kv[1]), StandardCopyOption.REPLACE_EXISTING)
                    }
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex)
        }
    }

    private String getPathFromDependency(final Map<String, Object> dep) {
        def fileSep = System.getProperty("file.separator")
        return this.extensionDirectory + fileSep + (String) dep.module
    }

    private def makeDepsMap(final Artifact artifact) {
        final Map<String, Object> map = new HashMap<>()
        map.put("classLoader", this.classLoaderToUse)
        map.put("group", artifact.getGroup())
        map.put("module", artifact.getArtifact())
        map.put("version", artifact.getVersion())
        map.put("changing", false)
        return map
    }

    private static void getFileNames(final List fileNames, final Path dir) {
        final DirectoryStream<Path> stream = Files.newDirectoryStream(dir)
        for (Path path : stream) {
            if (path.toFile().isDirectory()) getFileNames(fileNames, path)
            else {
                fileNames.add(path.toAbsolutePath())
            }
        }
        stream.close()
    }
}
