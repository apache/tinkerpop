requirejs.config({
    baseUrl: "/js",
    paths: {
        "bootstrap": "lib/bootstrap.min",
        "cookie": "lib/jquery.cookie",
        "dust": "lib/dust/dust-full-1.1.1",
        "dust-helpers": "lib/dust/dust-helpers-1.1.0",
        "jquery": "lib/jquery-1.9.1.min",
        "moment": "lib/moment.min",
        "mustache": "lib/mustache",
        "underscore": "lib/underscore.min",
        "uri": "lib/URI.min"
    },
    shim: {
        "bootstrap": ["jquery"],
        "dust-helpers": ["dust"]
    },
    locale: "en-us"
});