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
var MIME_TYPE_JSON = "application/json";
var STATUS_BAR_HEIGHT = 23;

var _template = new tinkerpop.util.Template();
var _socket = null;
var _sessionId = $.uuid();
var _requestId;
var _index = 0;
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
                $("#progress").hide();
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
        else {
            _socket.onmessage = undefined;
        }

        _socket.send(JSON.stringify(request));
    }
    else {


        _connectWebSocket(function () {
            if (!_.isUndefined(callback) && (callback != null)) {
                _socket.onmessage = callback;
            }
            else {
                _socket.onmessage = undefined;
            }

            _socket.onerror = function (evt) {
                $("#status").html("The connection to the Gremlin server was lost.");
            };

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
        _clearStatus();
        $("#progress").hide();
        $("#mainInput").show();
        alert("Unable to connect to the Gremlin server.");
        _showInput();
    };
}

function _clearStatus() {
    $("#status").html("");
    $("#statusInput").val("");
    $("#statusInput").hide();
}

function _showInput() {
    $("#mainInput textarea").focus();
    $("#mainInput textarea").selectRange($("#mainInput textarea").val().length);
    _mode = MODE.input;
}

function _closeOverlay() {
    $("#overlay").hide();
    $("#main").show();
    _clearStatus();
    _showInput();
}

function _initSearch() {
    $("#inputSearch").focus();

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

function _renderSearch(query) {
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
}

function _processImport() {
    var value = $("#inputImport").val().trim();
    if (value != "") {
        var static = $("#checkStaticImport").is(":checked");

        if (value.toLowerCase().slice(0, 13) == "import static") {
            // nothing to do
        }
        else if (value.toLowerCase().slice(0, 6) == "import") {
            if (static) {
                value = "import static " + value.substr(6);
            }
        }
        else {
            value = "import " + (static ?  "static " : "") + value;
        }

        var args = {};
        args[tinkerpop.mapping.arg.imports] = [value];
        args[tinkerpop.mapping.arg.accept] = MIME_TYPE_JSON;

        _sendRequest(tinkerpop.mapping.op.import, args);

        $("#inputImport").val("");
        $("#checkStaticImport").removeAttr("checked");
        $("#inputImport").focus();

        _renderImports();
    }
}

function _initImports() {
    $("#inputImport").focus();

    $("#inputImport").off();
    $("#inputImport").on("keyup", function (e) {
        var code = e.which;
        switch (code) {
            case 13:    // enter
                _processImport();
                break;
        }
    });

    $("#buttonAddImport").off();
    $("#buttonAddImport").on("click", function () {
        _processImport();
    });
}

function _renderImports() {
    var args = {};
    args[tinkerpop.mapping.arg.infoType] = tinkerpop.mapping.infoType.imports;
    args[tinkerpop.mapping.arg.accept] = MIME_TYPE_JSON;

    _sendRequest(
        tinkerpop.mapping.op.show,
        args,
        function (evt) {
            if (typeof evt.data === "string") {
                var response = JSON.parse(evt.data);
                if (response.code == 200) {
                    var requestId = response.requestId;
                    if (requestId == _requestId) {
                        var data = response.result["gremlin-groovy"][0];

                        // combine imports and extra imports
                        var imports = data.imports.concat(data.extraImports);

                        // separate java imports
                        var javaImports = _.chain(imports)
                            .filter(function (item) { return item.slice(0, 4) == "java"; })
                            .map(function (item) { return "import " + item; })
                            .value();

                        var otherImports = _.chain(imports)
                            .reject(function (item) { return item.slice(0, 4) == "java"; })
                            .map(function (item) { return "import " + item; })
                            .value();

                        var staticJavaImports = _.chain(data.extraStaticImports)
                            .filter(function (item) { return item.slice(0, 4) == "java"; })
                            .map(function (item) { return "import static " + item; })
                            .value();

                        var staticOtherImports = _.chain(data.extraStaticImports)
                            .reject(function (item) { return item.slice(0, 4) == "java"; })
                            .map(function (item) { return "import static " + item; })
                            .value();

                        $("#sectionImport").empty();

                        if (otherImports.length > 0) {
                            $("#sectionImport").append(Mustache.render(_template.get("list"), { data: otherImports }));
                            $("#sectionImport").append("<br />");
                        }

                        if (javaImports.length > 0) {
                            $("#sectionImport").append(Mustache.render(_template.get("list"), { data: javaImports }));
                            $("#sectionImport").append("<br />");
                        }

                        if (staticOtherImports.length > 0) {
                            $("#sectionImport").append(Mustache.render(_template.get("list"), { data: staticOtherImports }));
                            $("#sectionImport").append("<br />");
                        }

                        if (staticJavaImports.length > 0) {
                            $("#sectionImport").append(Mustache.render(_template.get("list"), { data: staticJavaImports }));
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
        if (_(json.dependency).has("groupId")) {
            // maven format
            dependency = {};
            dependency[tinkerpop.mapping.coordinate.group] = json.dependency.groupId;
            dependency[tinkerpop.mapping.coordinate.artifact] = json.dependency.artifactId;
            dependency[tinkerpop.mapping.coordinate.version] = json.dependency.version;
        }
        else if (_(json.dependency).has("_org")) {
            // ivy format
            dependency = {};
            dependency[tinkerpop.mapping.coordinate.group] = json.dependency._org;
            dependency[tinkerpop.mapping.coordinate.artifact] = json.dependency._name;
            dependency[tinkerpop.mapping.coordinate.version] = json.dependency._rev;
        }
    }
    catch (error) {
        dependency = null;
    }

    // try parsing as grape
    if (dependency == null) {
        var group = tinkerpop.util.parseGrape(str, "group");
        if (group != "") {
            dependency = {};
            dependency[tinkerpop.mapping.coordinate.group] = group;
            dependency[tinkerpop.mapping.coordinate.artifact] = tinkerpop.util.parseGrape(str, "module");
            dependency[tinkerpop.mapping.coordinate.version] = tinkerpop.util.parseGrape(str, "version");
        }
    }

    // try parsing as gradle
    if (dependency == null) {
        str = str.replace(/'/g, "");
        var parts = str.split(":");
        if (parts.length >= 3) {
            dependency = {};
            dependency[tinkerpop.mapping.coordinate.group] = parts[0];
            dependency[tinkerpop.mapping.coordinate.artifact] = parts[1];
            dependency[tinkerpop.mapping.coordinate.version] = parts[2];
        }
    }

    // validate dependency
    if (dependency != null) {
        if (!_(dependency).has(tinkerpop.mapping.coordinate.group) ||
            !_(dependency).has(tinkerpop.mapping.coordinate.artifact) ||
            !_(dependency).has(tinkerpop.mapping.coordinate.version)) {
            dependency = null;
        }
    }

    return dependency;
}

function _initDependencies() {
    $("#inputDependency").focus();

    $("#buttonAddDependency").off();
    $("#buttonAddDependency").on("click", function () {
        var str = $("#inputDependency").val().trim();
        if (str != "") {
            var dependency = _parseDependency(str);

            if (dependency == null) {
                alert("Could not parse dependency");
            }
            else {
                var args = {};
                args[tinkerpop.mapping.arg.coordinates] = [dependency];
                args[tinkerpop.mapping.arg.accept] = MIME_TYPE_JSON;

                _sendRequest(tinkerpop.mapping.op.use, args, function (evt) {
                    if (typeof evt.data === "string") {
                        // parse request id
                        var parts = evt.data.split(">>");
                        if (parts.length > 0) {
                            var requestId = parts[0];

                            if (requestId == _requestId) {
                                if (parts.length > 1) {
                                    $("#sectionDependency ul").append('<li style="padding-left: 10px">' + parts[1] + '</li>');
                                }
                            }
                        }
                    }
                });
            }
        }
    });
}

function _renderDependencies() {
    var args = {};
    args[tinkerpop.mapping.arg.infoType] = tinkerpop.mapping.infoType.dependencies;
    args[tinkerpop.mapping.arg.accept] = MIME_TYPE_JSON;

    _sendRequest(
        tinkerpop.mapping.op.show,
        args,
        function (evt) {
            if (typeof evt.data === "string") {
                var response = JSON.parse(evt.data);
                if (response.code == 200) {
                    var requestId = response.requestId;
                    if (requestId == _requestId) {
                        var data = response.result["gremlin-groovy"];
                        $("#sectionDependency").html(Mustache.render(_template.get("dependencyList"), { data: data }));
                    }
                }
            }
        }
    );
}

$(document).ready(function () {
    $("#main").height(window.innerHeight - STATUS_BAR_HEIGHT);
    $("#overlay").height(window.innerHeight - STATUS_BAR_HEIGHT);

    $(window).on("resize", function () {
        $("#main").height(window.innerHeight - STATUS_BAR_HEIGHT);
        $("#overlay").height(window.innerHeight - STATUS_BAR_HEIGHT);
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
                break;

           case 67:    // ctrl-c
                if (e.ctrlKey && _mode == MODE.execute) {
                    e.preventDefault();
                    _mode = MODE.cancel;
                    $(window).focus();
                    $("#status").html("Ctrl-C Cancel query? [y/n]:");
                    $("#statusInput").show();
                    $("#statusInput").focus();
                }
                break;

            case 72:    // ctrl-h
                if (e.ctrlKey) {
                    e.preventDefault();
                    _mode = MODE.help;
                    $("#status").html("Ctrl-H");
                    $("#main").hide();
                    $("#overlay").html(_template.get("help"));
                    $("#overlay").show();
                }
                break;

            case 73:    // ctrl-i
                if (e.ctrlKey) {
                    e.preventDefault();
                    _mode = MODE.import;
                    $("#status").html("Ctrl-I");
                    $("#main").hide();
                    $("#overlay").html(_template.get("import"));
                    $("#overlay").show();
                    _initImports();
                    _renderImports();
                }
                break;

            case 76:    // ctrl-l
                if (e.ctrlKey) {
                    e.preventDefault();
                    _mode = MODE.clear;
                    $(window).focus();
                    $("#status").html("Ctrl-L Clear history? [y/n]:");
                    $("#statusInput").show();
                    $("#statusInput").focus();
                }
                break;

            case 82:    // ctrl-r
                if (e.ctrlKey) {
                    e.preventDefault();
                    _mode = MODE.reset;
                    $(window).focus();
                    $("#status").html("Ctrl-R Reset? [y/n]:");
                    $("#statusInput").show();
                    $("#statusInput").focus();
                }
                break;

            case 83:    // ctrl-s
                if (e.ctrlKey) {
                    e.preventDefault();
                    _mode = MODE.search;
                    $("#status").html("Ctrl-S");
                    $("#main").hide();
                    $("#overlay").html(_template.get("search"));
                    $("#overlay").show();
                    _initSearch();
                    _renderSearch("");
                }
                break;

            case 85:    // ctrl-u
                if (e.ctrlKey) {
                    e.preventDefault();
                    _mode = MODE.use;
                    $("#status").html("Ctrl-U");
                    $("#main").hide();
                    $("#overlay").html(_template.get("dependency"));
                    $("#overlay").show();
                    _initDependencies();
                    _renderDependencies();
                }
                break;
        }
    });

    $(window).on("keyup", function (e) {
        var code = e.which;
        switch (code) {
            case 17:    // ctrl
                if ($("#status").html() == "Ctrl") {
                    $("#status").html("");
                }
                break;

            case 27:    // escape
                _closeOverlay();
                break;
       }
    });

    $("#statusInput").on("keyup", function (e) {
        var code = e.which;

        if (!e.ctrlKey) {
            switch (_mode) {
                case MODE.cancel:
                    if (code == 89) {       // y key
                        _sendRequest(tinkerpop.mapping.op.cancel, null, function (evt) {
                        });

                        _clearStatus();
                        _showInput();
                    }
                    else if (code == 78) {  // n key
                        _clearStatus();
                        _mode = MODE.execute;
                    }
                    break;

                case MODE.reset:
                    if (code == 89) {       // y key
                        _sendRequest(tinkerpop.mapping.op.reset);
                        _clearStatus();
                        _showInput();
                    }
                    else if (code == 78) {  // n key
                        _clearStatus();
                        _showInput();
                    }
                    break;

                case MODE.clear:
                    if (code == 89) {       // y key
                        _clearHistory();
                        _clearStatus();
                        _showInput();
                    }
                    else if (code == 78) {  // n key
                        _clearStatus();
                        _showInput();
                    }
                    break;
            }
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

                    $("#progress").show();
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
                if (!e.ctrlKey) {
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
            $("#mainInput textarea").focus();
        });

    } else {
        alert("Your browser does not support web sockets.");
    }
});
