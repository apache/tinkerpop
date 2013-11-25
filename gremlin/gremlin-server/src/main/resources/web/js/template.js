tinkerpop.namespace("tinkerpop.util.Template");
tinkerpop.util.Template = (function ($, _) {
    var _constr;
    var _templates = {};

    _constr = function () {
    };

    _constr.prototype = {
        constructor: tinkerpop.util.Template,
        get: function (name) {
            if (!_(_templates).has(name)) {
                $.ajax({
                    url: "template/" + name + ".html",
                    dataType: "text",
                    async: false,
                    type: "GET",
                    success: function (data) {
                        _templates[name] = data;
                    },
                    error: function (jqXHR, textStatus, errorThrown) {
                    }
                });
            }

            return _templates[name];
        }
    };

    return _constr;
})(jQuery, _);