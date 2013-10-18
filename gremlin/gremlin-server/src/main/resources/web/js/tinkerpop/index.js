require(
    [
        "domReady",
        "tinkerpop/template",
        "mustache",
        "bootstrap",
        "jquery",
        "dust",
        "underscore",
        "moment",
        "uri",
        "jquery-layout",
        "uuid"
    ],
    function (domReady, Template, mustache) {
        var _template = new Template();
        var _socket;
        var _sessionId = $.uuid();
        var _replRequestId = $.uuid();

        function _sendSocket (requestId, op, args) {
            var request = {
                "op": op,
                "sessionId": _sessionId,
                "requestId": requestId,
            };

            if (!_.isUndefined(args)) {
                request.args = args;
            }

            if (_socket != null && _socket.readyState == WebSocket.OPEN) {
                _socket.send(JSON.stringify(request));
            }
            else {
                _connectSocket(function () {
                    if (_socket != null && _socket.readyState == WebSocket.OPEN) {
                        _socket.send(JSON.stringify(request));
                    }
                });
            }
        }

        function _connectSocket (callback) {
            var pageUri = new URI(window.location.href);
            var socketUri = "ws://" + pageUri.hostname() + ":" + pageUri.port()  + "/gremlin";

            _socket = new WebSocket(socketUri);

            _socket.onmessage = function (evt) {
                // parse request id
                var dataParts = evt.data.split(">>");
                if (dataParts.length > 0) {
                    var requestId = dataParts[0];

                    if (dataParts.length > 1) {
                        var dataLines = dataParts[1].split(/\r?\n/);

                        _(dataLines).each(function (line) {
                            if (requestId == _replRequestId) {
                                $("#replConsole").append('<pre class="repl-text" style="margin: 0; padding: 0; border: 0">' + line + '</pre>');
                            }
                        })

                        var container = $(".ui-layout-south");
                        container[0].scrollTop = container[0].scrollHeight;
                    }
                }
            };

            _socket.onopen = function (evt) {
                if (!_.isUndefined(callback)) {
                    callback();
                }
            };

            _socket.onclose = function (evt) {
                _socket = null;
            };

            _socket.onerror = function (evt) {
                alert("An error occurred while connecting to the Gremlin server: " + evt.data)
            };
        }

        domReady(function () {  
            $(".dropdown-toggle").dropdown();

            var layout = $("#mainContainer").layout({
                onresize_end: function (pane, element, state, options, layout) {
                    if (pane == "center") {
                        $("#editContainer").height(state.innerHeight - 50);
                    }
                    else if (pane == "south") {
                        $("#replContainer").height(state.innerHeight);
                    }
                }
            });

            layout.sizePane("south", 150);

            $("#replPrompt textarea").keydown(function (e) {
                var code = e.which;
                if (code == 13) {
                    e.preventDefault();

                    var command = $(this).val().trim();
                    if (command != "") {
                        $("#replConsole").append('<pre class="repl-text" style="margin: 0; padding: 0; border: 0">gremlin&gt;&nbsp;' + command + '</pre>');
                        _sendSocket(_replRequestId, "eval", { "gremlin": command });
                        $(this).val("");
                    }
                }
            });

            if (!window.WebSocket) {
                window.WebSocket = window.MozWebSocket;
            }

            if (window.WebSocket) {
                _sendSocket(_replRequestId, "version", { "verbose" : true });

            } else {
                alert("Your browser does not support web sockets.");
            }
        });
    }
);
