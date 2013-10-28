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
        "uuid",
        "cookie"
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
        var _layout;
        var _preferences;


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

            $("#replDetail li:first").removeClass("repl-text");
            $("#replDetail li:first").addClass("repl-text-inverted");

            $("#replDetail li").unbind();
            $("#replDetail li").click(function () {
                $("#replPrompt textarea").val($(this).html());
                _closeReplDetail();
            })
        }

        function _initLayout(preferences) {
            if (preferences.centerContent != "") {
                $(".ui-layout-center").append($("#" + preferences.centerContent));
            }

            if (preferences.southContent != "") {
                $(".ui-layout-south").append($("#" + preferences.southContent));
            }

            if (preferences.eastContent != "") {
                $(".ui-layout-east").append($("#" + preferences.eastContent));
            }

            if (_.isUndefined(_layout)) {
                var options = {
                    onresize_end: function (pane, element, state, options, layout) {
                        if (!_.isUndefined(element[0].children.editorContainer)) {
                            $("#textContainer").height(state.innerHeight - 50);
                        }
                        else {
                            $("#replMain").height(state.innerHeight - 20);
                            $("#replDetail").height(state.innerHeight - 20);
                        }

                        switch (pane) {
                            case "south":
                                _preferences.southSize = state.innerHeight;
                                _savePreferences(_preferences);
                                break;

                            case "east":
                                _preferences.eastSize = state.innerWidth;
                                _savePreferences(_preferences);
                                break;
                        }
                    },
                    onclose_end: function (pane, element, state, options, layout) {
                        switch (pane) {
                            case "south":
                                _preferences.southClosed = true;
                                _savePreferences(_preferences);
                                break;

                            case "east":
                                _preferences.eastClosed = true;
                                _savePreferences(_preferences);
                                break;
                        }
                    },
                    onopen_end: function (pane, element, state, options, layout) {
                        switch (pane) {
                            case "south":
                                _preferences.southClosed = false;
                                _savePreferences(_preferences);
                                break;

                            case "east":
                                _preferences.eastClosed = false;
                                _savePreferences(_preferences);
                                break;
                        }
                    }
                };

                if (preferences.southSize > 0) {
                    options.south__size = preferences.southSize;
                }

                if (preferences.eastSize > 0) {
                    options.east__size = preferences.eastSize;
                }

                _layout = $("#mainContainer").layout(options);
            }
            else {
                if (preferences.southSize > 0) {
                    _layout.sizePane("south", preferences.southSize);
                }


                if (preferences.eastSize > 0) {
                    _layout.sizePane("east", preferences.eastSize);
                }
            }

            if (!_.isUndefined(_layout)) {
                if (preferences.southContent == "") {
                    _layout.hide("south");
                }
                else {
                    _layout.show("south");
                }

                if (preferences.southClosed) {
                    _layout.close("south");
                }
                else {
                    _layout.open("south");
                }

                if (preferences.eastContent == "") {
                    _layout.hide("east");
                }
                else {
                    _layout.show("east");
                }

                if (preferences.eastClosed) {
                    _layout.close("east");
                }
                else {
                    _layout.open("east");
                }

                _layout.resizeAll();
            }
        }

        function _loadPreferences(forceDefault) {
            var preferencesStr = $.cookie("preferences");

            if (!_.isUndefined(preferencesStr) && !forceDefault) {
                return JSON.parse(preferencesStr);
            }
            else {
                // default layout is editor in center pane and REPL in south pane
                return {
                    centerContent: "editorContainer",
                    southContent: "replContainer",
                    eastContent: "",
                    southSize: 200,
                    eastSize: 300,
                    southClosed: false,
                    eastClosed: true
                };
            }
        }

        function _savePreferences(preferences) {
            $.cookie("preferences", JSON.stringify(preferences), { expires: 365, path: "/" });
        }

        domReady(function () {  
            $(".dropdown-toggle").dropdown();

            _preferences = _loadPreferences();
            _initLayout(_preferences);

            $(window).resize(function () {
                _layout.resizeAll();
            });

            $("#btnColumn").click(function () {
                var content = $(".ui-layout-south > div");
                if (content.length > 0) {
                    $(".ui-layout-east").append(content);
                    _layout.show("east");
                    _layout.hide("south");
                    _layout.resizeAll();

                    _preferences.southContent = "";
                    _preferences.eastContent = content[0].id;
                    _savePreferences(_preferences);
                }
            });

            $("#btnRow").click(function () {
                var content = $(".ui-layout-east > div");
                if (content.length > 0) {
                    $(".ui-layout-south").append(content);
                    _layout.show("south");
                    _layout.hide("east");
                    _layout.resizeAll();

                    _preferences.southContent = content[0].id;
                    _preferences.eastContent = "";
                    _savePreferences(_preferences);
                }
            });

            $("#btnSwap").click(function () {
                var southContent = $(".ui-layout-south > div");
                var eastContent = $(".ui-layout-east > div");

                if (southContent.length > 0) {
                    $(".ui-layout-center").append(southContent);
                    var centerContent = $(".ui-layout-center > div:first");
                    $(".ui-layout-south").append(centerContent);
                    _layout.resizeAll();

                    _preferences.centerContent = southContent[0].id;
                    _preferences.southContent = centerContent[0].id;
                    _savePreferences(_preferences);
                }
                else if (eastContent.length > 0) {
                    $(".ui-layout-center").append(eastContent);
                    var centerContent = $(".ui-layout-center > div:first");
                    $(".ui-layout-east").append(centerContent);
                    _layout.resizeAll();

                    _preferences.centerContent = eastContent[0].id;
                    _preferences.eastContent = centerContent[0].id;
                    _savePreferences(_preferences);
                }
            });

            $("#listMenu li a").click(function () {
                var text = $(this).text();

                switch (text) {
                    case "Reset Layout":
                        _preferences = _loadPreferences(true);
                        _initLayout(_preferences);
                        _savePreferences(_preferences);
                        break;
                }
            });

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

            $("#replStatus div:nth-child(2) > input").keydown(function (e) {
                var code = e.which;
                switch (code) {
                    case 13:    // enter
                    case 38:    // up arrow
                    case 40:    // down arrow
                        e.preventDefault();
                        break;
                }
            });

            $("#replStatus div:nth-child(2) > input").keyup(function (e) {
                var code = e.which;
                switch (code) {
                    case 13:    // enter
                        var selection = $("#replDetail li.repl-text-inverted");
                        if (selection.length > 0) {
                           $("#replPrompt textarea").val(selection.html());
                           _closeReplDetail();
                        }
                        break;

                    case 38:    // up arrow
                        var selection = $("#replDetail li.repl-text-inverted");
                        if (selection.length > 0) {
                            var prev = selection.prev();
                            if (prev.length > 0) {
                                selection.removeClass("repl-text-inverted");
                                prev.removeClass("repl-text");
                                prev.addClass("repl-text-inverted");
                            }
                            else {
                                var last = $("#replDetail li:last");
                                selection.removeClass("repl-text-inverted");
                                last.removeClass("repl-text");
                                last.addClass("repl-text-inverted");
                            }                        }
                        break;

                    case 40:    // down arrow
                        var selection = $("#replDetail li.repl-text-inverted");
                        if (selection.length > 0) {
                            var next = selection.next();
                            if (next.length > 0) {
                                selection.removeClass("repl-text-inverted");
                                next.removeClass("repl-text");
                                next.addClass("repl-text-inverted");
                            }
                            else {
                                var first = $("#replDetail li:first");
                                selection.removeClass("repl-text-inverted");
                                first.removeClass("repl-text");
                                first.addClass("repl-text-inverted");
                            }
                        }
                        break;

                    default:
                        var query = $(this).val().trim();
                        _renderReplSearch(query);
                        break;
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
