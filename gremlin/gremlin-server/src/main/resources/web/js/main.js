requirejs.config({
    baseUrl: "/js",
    paths: {
        "bootstrap": "lib/bootstrap.min",
        "jquery": "lib/jquery-1.9.1.min",
        "moment": "lib/moment.min",
        "mustache": "lib/mustache",
        "underscore": "lib/underscore.min",
        "uri": "lib/URI.min",
        "uuid": "lib/jquery-uuid"
    },
    shim: {
        "bootstrap": ["jquery"],
        "uuid": ["jquery"]
    },
    locale: "en-us"
});