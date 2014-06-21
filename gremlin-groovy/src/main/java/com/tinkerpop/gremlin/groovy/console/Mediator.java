package com.tinkerpop.gremlin.groovy.console;

import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Mediator {
	public final Map<String,GremlinPlugin> loadedPlugins = new HashMap<>();
    public final List<RemoteAcceptor> remotes = new ArrayList<>();
	public int position;

    public RemoteAcceptor currentRemote() {
        return remotes.get(position);
    }

	public void addRemote(final RemoteAcceptor remote) {
		remotes.add(remote);
		position = remotes.size() - 1;
	}

	public RemoteAcceptor removeCurrent() {
		final RemoteAcceptor toRemove = remotes.remove(position);
		position = 0;
		return toRemove;
	}

	public RemoteAcceptor nextRemote() {
		position++;
		if (position >= remotes.size())
			position = 0;
		return currentRemote();
	}

	public RemoteAcceptor previousRemote() {
		position--;
		if (position < 0)
			position = remotes.size() - 1;
		return currentRemote();
	}

    public Object submit(final List<String> args) throws Exception {
        return currentRemote().submit(args);
    }

    public CompletableFuture<Void> close() {
        remotes.forEach(remote -> {
			try {
				remote.close();
			} catch (Exception ignored) {

			}
		});
        return CompletableFuture.completedFuture(null);
    }


}
