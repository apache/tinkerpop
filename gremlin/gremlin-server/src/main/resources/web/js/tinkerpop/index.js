require(
    [
        "domReady",
        "tinkerpop/template",
        "mustache",
        "bootstrap",
        "jquery",
        "dust",
        "moment",
        "uri"
    ],
    function (domReady, Template, mustache) {
        var _template = new Template();
        var socket;

        function send(message) {
            if (!window.WebSocket) { return; }
            if (socket.readyState == WebSocket.OPEN) {
                var json = {
                    "op": "eval",
                    "sessionId": "76CEC996-20C2-4766-B2EC-956EB743D46C",
                    "requestId": "F6D42765-7F81-45EA-B355-240DC71C8D33",
                    "args": { "gremlin": message }
                };

                socket.send(JSON.stringify(json));
            }
            else {
                alert("The socket is not open.");
            }
        }

        domReady(function () {  
            $(".dropdown-toggle").dropdown();

            if (!window.WebSocket) {
                window.WebSocket = window.MozWebSocket;
            }

            if (window.WebSocket) {
                var pageUri = new URI(window.location.href);
                var socketUri = "ws://" + pageUri.hostname() + ":" + pageUri.port()  + "/gremlin";

                socket = new WebSocket(socketUri);

                socket.onmessage = function(event) {
                    $("#responseText").val($("#responseText").val() + "\n" + event.data);
                };

                socket.onopen = function(event) {
                    $("#responseText").val("Web Socket opened!");
                };

                socket.onclose = function(event) {
                    $("#responseText").val("Web Socket closed");
                };
            } else {
                alert("Your browser does not support Web Socket.");
            }

            $("#btnEvaluate").click(function () {
                send($("#inputGremlin").val());
            });
        });
    }
);
