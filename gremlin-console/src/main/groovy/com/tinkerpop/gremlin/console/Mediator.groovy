package com.tinkerpop.gremlin.console

import com.tinkerpop.gremlin.console.plugin.PluggedIn
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor

import java.util.concurrent.CompletableFuture

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Mediator {
    public final Map<String, PluggedIn> availablePlugins = [:]
    public final List<RemoteAcceptor> remotes = []
    public int position

    public RemoteAcceptor currentRemote() { return remotes.get(position) }

    def addRemote(final RemoteAcceptor remote) {
        remotes.add(remote)
        position = remotes.size() - 1
        return remote
    }

    def removeCurrent() {
        final RemoteAcceptor toRemove = remotes.remove(position)
        position = 0
        return toRemove
    }

    def RemoteAcceptor nextRemote() {
        position++
        if (position >= remotes.size()) position = 0
        return currentRemote()
    }

    def RemoteAcceptor previousRemote() {
        position--
        if (position < 0) position = remotes.size() - 1
        return currentRemote()
    }

    def submit(final List<String> args) throws Exception { return currentRemote().submit(args) }

    def writePluginState() {
        def fileSep = System.getProperty("file.separator")
        def lineSep = System.getProperty("line.separator")
        def extPath = System.getProperty("user.dir") + fileSep + "ext" + fileSep + "plugins.txt"
        def file = new File(extPath)
        if (file.exists()) file.delete()

        new File(extPath).withWriter { out ->
            availablePlugins.findAll{it.value.activated}.each{ k, v -> out << (k + lineSep)}
        }
    }

    static def readPluginState() {
        def fileSep = System.getProperty("file.separator")
        def extPath = System.getProperty("user.dir") + fileSep + "ext" + fileSep + "plugins.txt"
        def file = new File(extPath)
        return file.exists() ? file.readLines() : []
    }

    def CompletableFuture<Void> close() {
        remotes.each { remote ->
            try {
                remote.close()
            } catch (Exception ignored) {

            }
        }

        return CompletableFuture.completedFuture(null)
    }

}
