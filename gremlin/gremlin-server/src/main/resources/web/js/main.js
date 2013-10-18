requirejs.config({
    baseUrl: "/js",
    paths: {
        "bootstrap": "lib/bootstrap.min",
        "cookie": "lib/jquery.cookie",
        "dust": "lib/dust/dust-full-1.1.1",
        "dust-helpers": "lib/dust/dust-helpers-1.1.0",
        "jquery": "lib/jquery-1.9.1.min",
        "jquery-layout": "lib/jquery.layout-latest.min",
        "jquery-ui": "lib/jquery-ui-1.10.3.custom.min",
        "moment": "lib/moment.min",
        "mustache": "lib/mustache",
        "underscore": "lib/underscore.min",
        "uri": "lib/URI.min",
        "uuid": "lib/jquery-uuid"
    },
    shim: {
        "bootstrap": ["jquery"],
        "dust-helpers": ["dust"],
        "jquery-layout": ["jquery", "jquery-ui"],
        "uuid": ["jquery"]
    },
    locale: "en-us"
});