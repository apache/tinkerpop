var MODE = {
    input: 0,
    execute: 1,
    cancel: 2,
    help: 3,
    search: 4,
    clear: 5,
    use: 6,
    import: 7,
    reset: 8
};

var COOKIE_NAME = "history";

var _template = new tinkerpop.util.Template();
var _socket = null;
var _sessionId = $.uuid();
var _requestId;
var _index = 0;
var _ctrlKey = false;
var _mode = MODE.input;

function _getHistory() {
    var historyStr = $.cookie(COOKIE_NAME);
    return (!_.isUndefined(historyStr)) ? JSON.parse(historyStr) : [""];
}

function _setHistory(item, index) {
    var history = _getHistory();

    if (!_.isUndefined(index)) {
        history[index] = item;
    }
    else {
        history.push(item);
    }

    $.cookie(COOKIE_NAME, JSON.stringify(history), { expires: 365, path: "/" });
}

function _clearHistory() {
    $.removeCookie(COOKIE_NAME, { path: "/" });
}

function _processEval(evt) {
    // determine the message type
    if (evt.data instanceof Blob) {
        // convert the blob into a uint8 array
        var fileReader = new FileReader();

        fileReader.onload = function(e) {
            var uint8Array = new Uint8Array(this.result);
            var requestId = tinkerpop.util.arrayToUuid(uint8Array);

            if (requestId == _requestId) {
                $("#mainInput").show();
                $("#mainInput textarea").focus();
                _mode = MODE.input;
            }
        };

        fileReader.readAsArrayBuffer(evt.data);
    }
    else if (typeof evt.data === "string") {
        // parse request id
        var parts = evt.data.split(">>");
        if (parts.length > 0) {
            var requestId = parts[0];

            if (requestId == _requestId) {
                if (parts.length > 1) {
                    var lines = parts[1].split(/\r?\n/);

                    _(lines).each(function (line) {
                        $("#mainConsole").append('<pre class="repl-text" style="margin: 0; padding: 0; border: 0">' + line + '</pre>');
                        var container = $("#main");
                        container[0].scrollTop = container[0].scrollHeight;
                    })
                }
            }
        }
    }
}

function _sendRequest(op, args, callback) {
    _requestId = $.uuid();

    var request = {
        "op": op,
        "sessionId": _sessionId,
        "requestId": _requestId,
    };

    if (!_.isUndefined(args) && (args != null)) {
        request.args = args;
    }

    if (_socket != null && _socket.readyState == WebSocket.OPEN) {
        if (!_.isUndefined(callback) && (callback != null)) {
            _socket.onmessage = callback;
        }

        _socket.send(JSON.stringify(request));
    }
    else {
        _connectWebSocket(function () {
            if (!_.isUndefined(callback) && (callback != null)) {
                _socket.onmessage = callback;
            }

            _socket.send(JSON.stringify(request));
        });
    }
}

function _connectWebSocket(onOpen) {
    var pageUri = new URI(window.location.href);
    var socketUri = "ws://" + pageUri.hostname() + ":" + pageUri.port()  + "/gremlin";

    _socket = new WebSocket(socketUri);

    _socket.onopen = function (evt) {
        if (!_.isUndefined(onOpen)) {
            onOpen();
        }
    };

    _socket.onclose = function (evt) {
        _socket = null;
    };

    _socket.onerror = function (evt) {
        $("#status").html("The connection to the Gremlin server was lost.")
    };
}

function _closeOverlay() {
    $("#overlay").hide();
    $("#main").show();
    $("#status").html("");
    $("#mainInput textarea").focus();
    $("#mainInput textarea").selectRange($("#mainInput textarea").val().length);
    _mode = MODE.input;
}

function _renderSearch(query) {
    $("#inputSearch").focus();

    var pattern = new RegExp(query.escapeRegExp(), "gi");
    var data = _.chain(_getHistory())
        .rest()
        .filter(function (item) {
            return item.match(pattern);
        })
        .value();

    $("#sectionSearch").html(Mustache.render(_template.get("list"), { data: data, cursor: "pointer" }));

    $("#sectionSearch li:first").removeClass("repl-text");
    $("#sectionSearch li:first").addClass("repl-text-inverted");

    $("#sectionSearch li").off();
    $("#sectionSearch li").on("click", function () {
        $("#mainInput textarea").val($(this).html());
        _closeOverlay();
    });

    $("#inputSearch").off();

    $("#inputSearch").on("keydown", function (e) {
        var code = e.which;
        switch (code) {
            case 13:    // enter
            case 38:    // up arrow
            case 40:    // down arrow
                e.preventDefault();
                break;
        }
    });

    $("#inputSearch").on("keyup", function (e) {
        var code = e.which;
        switch (code) {
            case 13:    // enter
                var selection = $("#sectionSearch li.repl-text-inverted");
                if (selection.length > 0) {
                   $("#mainInput textarea").val(selection.html());
                   _closeOverlay();
                }
                break;

            case 38:    // up arrow
                var selection = $("#sectionSearch li.repl-text-inverted");
                if (selection.length > 0) {
                    var prev = selection.prev();
                    if (prev.length > 0) {
                        selection.removeClass("repl-text-inverted");
                        prev.removeClass("repl-text");
                        prev.addClass("repl-text-inverted");
                    }
                    else {
                        var last = $("#sectionSearch li:last");
                        selection.removeClass("repl-text-inverted");
                        last.removeClass("repl-text");
                        last.addClass("repl-text-inverted");
                    }
                }
                break;

            case 40:    // down arrow
                var selection = $("#sectionSearch li.repl-text-inverted");
                if (selection.length > 0) {
                    var next = selection.next();
                    if (next.length > 0) {
                        selection.removeClass("repl-text-inverted");
                        next.removeClass("repl-text");
                        next.addClass("repl-text-inverted");
                    }
                    else {
                        var first = $("#sectionSearch li:first");
                        selection.removeClass("repl-text-inverted");
                        first.removeClass("repl-text");
                        first.addClass("repl-text-inverted");
                    }
                }
                break;

            default:
                var query = $(this).val().trim();
                _renderSearch(query);
                break;
        }
    });
}

function _renderImports() {
    $("#inputImport").focus();

    var args = {};
    args[tinkerpop.mapping.arg.infoType] = tinkerpop.mapping.infoType.imports;

    _sendRequest(
        tinkerpop.mapping.op.show,
        args,
        function (evt) {
            if (typeof evt.data === "string") {
                // parse request id
                var parts = evt.data.split(">>");
                if (parts.length > 0) {
                    var requestId = parts[0];

                    if (requestId == _requestId) {
                        if (parts.length > 1) {
                            var str = parts[1];
                            var start = str.indexOf("[[");
                            var end = str.indexOf("]]");
                            var imports = str.substr(start + 2, end - start - 2).split(",");

                            $("#sectionImport").html(Mustache.render(_template.get("list"), { data: imports, cursor: "none" }));

                            $("#inputImport").on("keyup", function (e) {
                                var code = e.which;
                                switch (code) {
                                    case 13:    // enter
                                        var value = $(this).val().trim();
                                        if (value != "") {
                                            args[tinkerpop.mapping.arg.imports] = value;
                                            _sendRequest(tinkerpop.mapping.op.import, args, function (evt) {
                                                $("#sectionImport").append('<li>' + value + '</li>');
                                            });
                                        }
                                        break;
                                }
                            });

                            $("#buttonAddImport").off();
                            $("#buttonAddImport").on("click", function () {
                                var value = $("#inputImport").val().trim();
                                if (value != "") {
                                    var args = {};
                                    args[tinkerpop.mapping.arg.imports] = value;
                                    _sendRequest(tinkerpop.mapping.op.import, args, function (evt) {
                                        $("#sectionImport").append('<li>' + value + '</li>');
                                    });
                                }
                            });
                        }
                    }
                }
            }
        }
    );
}

function _parseDependency(str) {
    var dependency = null;

    // try parsing as maven or ivy
    try {
        var x2js = new X2JS();

        var json = x2js.xml_str2json(str);
        if (!_.isUndefined(json.dependency.groupId)) {
            // maven format
            dependency = {
                groupId: json.dependency.groupId,
                artifactId: json.dependency.artifact,
                version: json.dependency.version
            };
        }
        else if (!_.isUndefined(json.dependency._org)) {
            // ivy format
            dependency = {
                groupId: json.dependency._org,
                artifactId: json.dependency._name,
                version: json.dependency._rev
            };
        }
    }
    catch (error) {
        dependency = null;
    }

    // try parsing as grape
    if (dependency == null) {
        var group = tinkerpop.util.parseGrape(str, "group");
        if (group != "") {
            dependency = {
                groupId: group,
                artifactId: tinkerpop.util.parseGrape(str, "module"),
                version: tinkerpop.util.parseGrape(str, "version")
            }
        }
    }

    // try parsing as gradle
    if (dependency == null) {
        str = str.replace(/'/g, "");
        var parts = str.split(":");
        if (parts.length >= 3) {
            dependency = {
                groupId: parts[0],
                artifactId: parts[1],
                version: parts[2]
            }
        }
    }

    return dependency;
}

function _renderDependencies() {
    $("#inputDependency").focus();

    var args = {};
    args[tinkerpop.mapping.arg.infoType] = tinkerpop.mapping.infoType.dependencies;

    _sendRequest(
        tinkerpop.mapping.op.show,
        args,
        function (evt) {
            if (typeof evt.data === "string") {
                // parse request id
                var parts = evt.data.split(">>");
                if (parts.length > 0) {
                    var requestId = parts[0];

                    if (requestId == _requestId) {
                        if (parts.length > 1) {
                            var str = parts[1];
                            var start = str.indexOf("[");
                            var end = str.indexOf("]");
                            var dependencies = str.substr(start + 1, end - start - 1);

                            $("#sectionDependency").html(Mustache.render(_template.get("list"), { data: dependencies, cursor: "none" }));

                            $("#buttonAddDependency").off();
                            $("#buttonAddDependency").on("click", function () {
                                var str = $("#inputDependency").val().trim();
                                if (str != "") {
                                    var args = {};
                                    var dependency = _parseDependency(str);

                                    if (dependency == null) {
                                        alert("Could not parse dependency");
                                    }
                                    else {
                                        args[tinkerpop.mapping.arg.dependencies] = dependency;
                                        _sendRequest(tinkerpop.mapping.op.use, args, function (evt) {

                                        });
                                    }
                                }
                            });
                        }
                    }
                }
            }
        }
    );
}

$(document).ready(function () {
    $("#main").height(window.innerHeight - 20);
    $("#overlay").height(window.innerHeight - 20);

    $(window).on("resize", function () {
        $("#main").height(window.innerHeight - 20);
        $("#overlay").height(window.innerHeight - 20);
    });

    $(window).on("beforeunload", function () {
        if (_socket != null) {
            _socket.close();
        }
    });

    $(window).on("keydown", function (e) {
        var code = e.which;
        switch (code) {
            case 27:    // escape
                e.preventDefault();
                break;

            case 17:    // ctrl
                $("#status").html("Ctrl");
                _ctrlKey = true;
                break;
        }

        if (_ctrlKey) {
            switch (code) {
                case 67:    // ctrl-c
                    e.preventDefault();
                    _mode = MODE.cancel;
                    $(window).focus();
                    $("#status").html("Ctrl-C Cancel query? [y/N]")
                    break;

                case 72:    // ctrl-h
                    e.preventDefault();
                    _mode = MODE.help;
                    $("#status").html("Ctrl-H");
                    $("#main").hide();
                    $("#overlay").html(_template.get("help"))
                    $("#overlay").show();
                    break;

                case 73:    // ctrl-i
                    e.preventDefault();
                    _mode = MODE.import;
                    $("#status").html("Ctrl-I");
                    $("#main").hide();
                    $("#overlay").html(_template.get("import"))
                    $("#overlay").show();
                    _renderImports();
                    break;

                case 76:    // ctrl-l
                    e.preventDefault();
                    _mode = MODE.clear;
                    $(window).focus();
                    $("#status").html("Ctrl-L Clear history? [y/N]")
                    break;

                case 82:    // ctrl-r
                    e.preventDefault();
                    _mode = MODE.reset;
                    $(window).focus();
                    $("#status").html("Ctrl-R Reset? [y/N]");
                    break;

                case 83:    // ctrl-s
                    e.preventDefault();
                    _mode = MODE.search;
                    $("#status").html("Ctrl-S");
                    $("#main").hide();
                    $("#overlay").html(_template.get("search"))
                    $("#overlay").show();
                    _renderSearch("");
                    break;

                case 85:    // ctrl-u
                    e.preventDefault();
                    _mode = MODE.use;
                    $("#status").html("Ctrl-U");
                    $("#main").hide();
                    $("#overlay").html(_template.get("dependency"))
                    $("#overlay").show();
                    _renderDependencies();
                    break;
            }
        }
    });

    $(window).on("keyup", function (e) {
        var code = e.which;
        switch (code) {
            case 17:    // ctrl
                _ctrlKey = false;

                if ($("#status").html() == "Ctrl") {
                    $("#status").html("");
                }
                break;

            case 27:    // escape
                _closeOverlay();
                break;

            default:
                switch (_mode) {
                    case MODE.cancel:
                        if (code == 89) {   // y key
                            _sendRequest(tinkerpop.mapping.op.cancel, null, function (evt) {

                            });

                            _closeOverlay();
                        } else if (code != 67) {    // need to filter out the c key from the ctrl-c hot key event
                            _closeOverlay();
                        }
                        break;

                    case MODE.reset:
                        if (code == 89) {   // y key
                            _sendRequest(tinkerpop.mapping.op.reset);
                            _closeOverlay();
                        } else if (code != 82) {    // need to filter out the r key from the ctrl-r hot key event
                            _closeOverlay();
                        }
                        break;

                    case MODE.clear:
                        if (code == 89) {   // y key
                            _clearHistory();
                            _closeOverlay();
                        } else if (code != 76) {    // need to filter out the l key from the ctrl-l hot key event
                            _closeOverlay();
                        }
                        break;
                }
                break;
        }

    });

    $("#mainInput textarea").on("keydown", function (e) {
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

    $("#mainInput textarea").on("keyup", function (e) {
        var code = e.which;
        switch (code) {
            case 9:     // tab
                var command = $(this).val().trim();
                if (command != "") {
                    // ignore index 0 which holds the active command string
                    var match = _.chain(_getHistory())
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
                    _setHistory("", 0);
                    _setHistory(command);

                    var args = {};
                    args[tinkerpop.mapping.arg.gremlin] = command;
                    _sendRequest(tinkerpop.mapping.op.eval, args, _processEval);

                    $("#mainConsole").append('<pre class="repl-text" style="margin: 0; padding: 0; border: 0">gremlin&gt;&nbsp;' + command + '</pre>');
                    $("#mainInput").hide();
                    $(this).val("");

                    _mode = MODE.execute;
                }
                break;

            case 38:    // up arrow
                var history = _getHistory();
                if (history.length > 0) {
                    _index = (_index < history.length - 1) ? _index + 1 : 0;
                    $(this).val(history[_index]);
                }
                break;

            case 40:    // down arrow
                var history = _getHistory();
                if (history.length > 0) {
                    _index = (_index > 0) ? _index - 1 : history.length - 1;
                    $(this).val(history[_index]);
                }
                break;

            default:
                if (!_ctrlKey) {
                    // index 0 holds the active command string
                    _setHistory($(this).val(), 0);
                }
                break;
        }
    });

    if (!window.WebSocket) {
        window.WebSocket = window.MozWebSocket;
    }

    if (window.WebSocket) {
        var args = {};
        args[tinkerpop.mapping.arg.verbose] = true;

        _sendRequest(tinkerpop.mapping.op.version, args, function (evt) {
            _processEval(evt);
        });

    } else {
        alert("Your browser does not support web sockets.");
    }
});
