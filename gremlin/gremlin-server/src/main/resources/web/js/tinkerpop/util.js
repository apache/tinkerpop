define(
    [
    ],
    function () {
        return {
            escapeRegExp: function (text) {
                return text.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&");
            }
        };
    }
);

