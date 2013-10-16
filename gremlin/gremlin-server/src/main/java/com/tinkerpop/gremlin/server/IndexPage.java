package com.tinkerpop.gremlin.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

/**
 * Adapted from https://github.com/netty/netty/tree/netty-4.0.10.Final/example/src/main/java/io/netty/example/http/websocketx/server
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class IndexPage {
    public static final String NEWLINE = "\r\n";

    public static ByteBuf getContent(String webSocketLocation) {
        return Unpooled.copiedBuffer(
                "<html><head><title>.:TinkerPop::Gremlin Server:.</title></head>" + NEWLINE +
                        "<body>" + NEWLINE +
                        "<script type=\"text/javascript\">(function(){var a=[];for(var b=0;b<=15;b++)a[b]=b.toString(16);var c=function(){var b=\"\";for(var c=1;c<=36;c++)c===9||c===14||c===19||c===24?b+=\"-\":c===15?b+=4:c===20?b+=a[Math.random()*4|8]:b+=a[Math.random()*15|0];return b};typeof exports!=\"undefined\"?module.exports=c:this.UUID=c})(this);</script>" + NEWLINE +
                        "<script type=\"text/javascript\">" + NEWLINE +
                        "var socket;" + NEWLINE +
                        "if (!window.WebSocket) {" + NEWLINE +
                        "  window.WebSocket = window.MozWebSocket;" + NEWLINE +
                        '}' + NEWLINE +
                        "if (window.WebSocket) {" + NEWLINE +
                        "  socket = new WebSocket(\"" + webSocketLocation + "\");" + NEWLINE +
                        "  socket.onmessage = function(event) {" + NEWLINE +
                        "    var ta = document.getElementById('responseText');" + NEWLINE +
                        "    ta.value = ta.value + '\\n' + event.data" + NEWLINE +
                        "  };" + NEWLINE +
                        "  socket.onopen = function(event) {" + NEWLINE +
                        "    var ta = document.getElementById('responseText');" + NEWLINE +
                        "    ta.value = \"Web Socket opened!\";" + NEWLINE +
                        "  };" + NEWLINE +
                        "  socket.onclose = function(event) {" + NEWLINE +
                        "    var ta = document.getElementById('responseText');" + NEWLINE +
                        "    ta.value = ta.value + \"Web Socket closed\"; " + NEWLINE +
                        "  };" + NEWLINE +
                        "} else {" + NEWLINE +
                        "  alert(\"Your browser does not support Web Socket.\");" + NEWLINE +
                        '}' + NEWLINE +
                        NEWLINE +
                        "function send(message) {" + NEWLINE +
                        "  if (!window.WebSocket) { return; }" + NEWLINE +
                        "  if (socket.readyState == WebSocket.OPEN) {" + NEWLINE +
                        "    var json = '{\"op\":\"eval\",\"sessionId\":\"76CEC996-20C2-4766-B2EC-956EB743D46C\",\"requestId\":\"' + UUID() + '\",\"args\":{\"gremlin\":\"' + message + '\", \"bindings\":{\"x\":1}}}';" + NEWLINE +
                        "    socket.send(json);" + NEWLINE +
                        "  } else {" + NEWLINE +
                        "    alert(\"The socket is not open.\");" + NEWLINE +
                        "  }" + NEWLINE +
                        '}' + NEWLINE +
                        "</script>" + NEWLINE +
                        "<form onsubmit=\"return false;\">" + NEWLINE +
                        "<h1>Gremlin</h1>" + NEWLINE +
                        "<input type=\"text\" name=\"message\" value=\"com.tinkerpop.gremlin.pipes.Gremlin.of(g).V().out().out()\" style=\"width:500px;\"/>" +
                        "<input type=\"button\" value=\"Evaluate\"" + NEWLINE +
                        "       onclick=\"send(this.form.message.value)\" />" + NEWLINE +
                        "<h3>Output</h3>" + NEWLINE +
                        "<textarea id=\"responseText\" style=\"width:500px;height:300px;\"></textarea>" + NEWLINE +
                        "</form>" + NEWLINE +
                        "</body>" + NEWLINE +
                        "</html>" + NEWLINE, CharsetUtil.US_ASCII);
    }

    private IndexPage() {
        // Unused
    }
}