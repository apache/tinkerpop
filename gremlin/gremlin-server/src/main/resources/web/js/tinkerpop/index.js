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
        "jquery-layout"
    ],
    function (domReady, Template, mustache) {
        var _template = new Template();
        var _socket;

        function _sendSocket(op, args) {
            if (!window.WebSocket) { return; }
            if (_socket == null || _socket.readyState == WebSocket.OPEN) {
                var json = {
                    "op": op,
                    "sessionId": "76CEC996-20C2-4766-B2EC-956EB743D46C",
                    "requestId": "F6D42765-7F81-45EA-B355-240DC71C8D33",
                };

                if (!_.isUndefined(args)) {
                    json.args = args;
                }

                _socket.send(JSON.stringify(json));
            }
            else {
                var result = confirm("Do you want to open a connection to the Gremlin server?");
                if (result == true) {
                    _connectSocket();
                }
            }
        }

        function _connectSocket() {
            var pageUri = new URI(window.location.href);
            var socketUri = "ws://" + pageUri.hostname() + ":" + pageUri.port()  + "/gremlin";

            _socket = new WebSocket(socketUri);

            _socket.onmessage = function(event) {
                var dataLines = event.data.split(/\r?\n/);

                _(dataLines).each(function (line) {
                    $("#replPrompt").before('<pre class="repl-text" style="margin: 0; padding: 0; border: 0">' + line + '</pre>');
                })

                $("#replPrompt").show();
            };

            _socket.onopen = function(event) {
                $("#replContainer").empty();
                $("#replContainer").append(_template.get("replPrompt"));

                $("#replPrompt textarea").keydown(function (e) {
                    var code = e.which;
                    if (code == 13) {
                        e.preventDefault();

                        var command = $(this).val().trim();
                        if (command != "") {
                            $("#replPrompt").hide();
                            $("#replPrompt").before('<pre class="repl-text" style="margin: 0; padding: 0; border: 0">gremlin&gt;&nbsp;' + command + '</pre>');

                            _sendSocket ("eval", { "gremlin": command });


                            $(this).val("");
                        }
                    }
                });

                _sendSocket("version", { "verbose": true });
            };

            _socket.onclose = function(event) {
                _socket = null;
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

            if (!window.WebSocket) {
                window.WebSocket = window.MozWebSocket;
            }

            if (window.WebSocket) {
                _connectSocket();

            } else {
                alert("Your browser does not support web sockets.");
            }
        });
    }
);
