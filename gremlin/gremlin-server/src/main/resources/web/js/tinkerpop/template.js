define(
    [
        "jquery",
        "dust",
        "dust-helpers",
        "underscore"
    ],
    function () {
        var Template = function () {
            var self = this;

            var _templates = {};

            $.extend(self, {
                get: function (name) {
                    if (!_(_templates).has(name)) {
                        $.ajax({
                            url: "template/" + name + ".html",
                            dataType: "text",
                            async: false,
                            type: "GET",
                            success: function (data) {
                                _templates[name] = data;

                                if (!data.match(/{{.*}}/)) {
                                    dust.loadSource(dust.compile(data, name));
                                }
                            },
                            error: function (jqXHR, textStatus, errorThrown) {
                            }
                        });
                    }

                    return _templates[name];
                }
            });
        };

        return Template;
    }
);