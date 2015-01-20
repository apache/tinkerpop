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

    private final Console console

    private static String FILE_SEP = System.getProperty("file.separator")
    private static String LINE_SEP = System.getProperty("line.separator")
    private static
    final String PLUGIN_CONFIG_FILE = System.getProperty("user.dir") + FILE_SEP + "ext" + FILE_SEP + "plugins.txt"

    public Mediator(final Console console) {
        this.console = console
    }

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

    def showShellEvaluationOutput(final boolean show) {
        console.showShellEvaluationOutput(show)
    }

    def submit(final List<String> args) throws Exception { return currentRemote().submit(args) }

    def writePluginState() {
        def file = new File(PLUGIN_CONFIG_FILE)

        // ensure that the directories exist to hold the file.
        file.mkdirs()

        if (file.exists())
            file.delete()

        new File(PLUGIN_CONFIG_FILE).withWriter { out ->
            availablePlugins.findAll { it.value.activated }.each { k, v -> out << (k + LINE_SEP) }
        }
    }

    static def readPluginState() {
        def file = new File(PLUGIN_CONFIG_FILE)
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
