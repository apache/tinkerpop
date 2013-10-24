require(
    [
        "domReady",
        "tinkerpop/template",
        "tinkerpop/jquery-util",
        "tinkerpop/util",
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
    function (domReady, Template, jqUtil, util, mustache) {
        var _template = new Template();
        var _socket;
        var _sessionId = $.uuid();
        var _replRequestId = $.uuid();
        var _replHistory = [""];
        var _replIndex = 0;
        var _replSearch = false;
        var _ctrlKey = false;

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
                                var container = $("#replMain");
                                container[0].scrollTop = container[0].scrollHeight;
                            }
                        })
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
                alert("An error occurred while connecting to the Gremlin server.")
            };
        }

        function _closeReplDetail() {
            $("#replDetail").hide();
            $("#replMain").show();
            $("#replStatus div:nth-child(1)").html("");
            $("#replStatus div:nth-child(2) > input").hide();
            $("#replPrompt textarea").focus();
            $("#replPrompt textarea").selectRange($("#replPrompt textarea").val().length);
        }

        function _renderReplSearch(query) {
            var pattern = new RegExp(util.escapeRegExp(query), "gi");
            var data = _.chain(_replHistory)
                .rest()
                .filter(function (item) {
                    return item.match(pattern);
                })
                .value();

            $("#replDetail").html(mustache.render(_template.get("searchResult"), data));
            $("#replDetail li").unbind();
            $("#replDetail li").click(function () {
                $("#replPrompt textarea").val($(this).html());
                _closeReplDetail();
            })
        }

        domReady(function () {  
            $(".dropdown-toggle").dropdown();

            var layout = $("#mainContainer").layout({
                onresize_end: function (pane, element, state, options, layout) {
                    if (pane == "center") {
                        $("#editContainer").height(state.innerHeight - 50);
                    }
                    else if (pane == "south") {
                        $("#replMain").height(state.innerHeight - 20);
                        $("#replDetail").height(state.innerHeight - 20);
                    }
                }
            });

            layout.sizePane("south", 190);

            $("body").keydown(function (e) {
                var code = e.which;
                switch (code) {
                    case 27:    // escape
                        e.preventDefault();
                        break;

                    case 17:    // ctrl
                        $("#replStatus div:nth-child(1)").html("Ctrl");
                        _ctrlKey = true;
                        break;
                }

                if (_ctrlKey) {
                    switch (code) {
                        case 72:    // ctrl-h
                            e.preventDefault();
                            $("#replStatus div:nth-child(1)").html("Ctrl-H Help");
                            $("#replMain").hide();
                            $("#replDetail").html(_template.get("help"))
                            $("#replDetail").show();
                            break;

                        case 83:    // ctrl-s
                            e.preventDefault();
                            $("#replStatus div:nth-child(1)").html("Ctrl-S Search:");
                            $("#replStatus div:nth-child(2) > input").val("");
                            $("#replStatus div:nth-child(2) > input").show();
                            $("#replMain").hide();
                            _renderReplSearch("");
                            $("#replDetail").show();
                            $("#replStatus div:nth-child(2) > input").focus();
                            break;
                    }
                }

            });

            $("body").keyup(function (e) {
                var code = e.which;
                switch (code) {
                    case 17:    // ctrl
                        _ctrlKey = false;

                        if ($("#replStatus div:nth-child(1)").html() == "Ctrl") {
                            $("#replStatus div:nth-child(1)").html("");
                        }
                        break;

                    case 27:    // escape
                        _closeReplDetail();
                        break;
                }
            });

            //$("body").keydown(function (e) {
            //    var code = e.which;
            //    console.log(code);
            //});


            $("#replPrompt textarea").keydown(function (e) {
                var code = e.which;
                switch (code) {
                    case 9:     // tab
                    case 13:    // enter
                    case 38:    // up arrow
                    case 40:    // down arrow
                        e.preventDefault();
                        break;
                }
            });

            $("#replPrompt textarea").keyup(function (e) {
                var code = e.which;
                switch (code) {
                    case 9:     // tab
                        var command = $(this).val().trim();
                        if (command != "") {
                            // ignore index 0 which holds the active command string
                            var match = _.chain(_replHistory)
                                .rest()
                                .find(function (item) {
                                    return (item.slice(0, command.length) == command);
                                })
                                .value();

                            if (!_.isUndefined(match)) {
                                $(this).val(match);
                            }
                        }
                        break;

                    case 13:    // enter
                        var command = $(this).val().trim();
                        if (command != "") {
                            _replHistory[0] = "";
                            _replHistory.push(command);
                            $("#replConsole").append('<pre class="repl-text" style="margin: 0; padding: 0; border: 0">gremlin&gt;&nbsp;' + command + '</pre>');
                            _sendSocket(_replRequestId, "eval", { "gremlin": command });
                            $(this).val("");
                        }
                        break;

                    case 38:    // up arrow
                        if (_replHistory.length > 0) {
                            _replIndex = (_replIndex < _replHistory.length - 1) ? _replIndex + 1 : 0;
                            $(this).val(_replHistory[_replIndex]);
                        }
                        break;

                    case 40:    // down arrow
                        if (_replHistory.length > 0) {
                            _replIndex = (_replIndex > 0) ? _replIndex - 1 : _replHistory.length - 1;
                            $(this).val(_replHistory[_replIndex]);
                        }
                        break;

                    default:
                        if (!_ctrlKey) {
                            // index 0 holds the active command string
                            _replHistory[0] = $(this).val();
                        }
                        break;
                }
            });

            $("#replStatus div:nth-child(2) > input").keyup(function (e) {
                var query = $(this).val().trim();
                _renderReplSearch(query);
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
