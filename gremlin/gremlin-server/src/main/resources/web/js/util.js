// jQuery functions
(function ($) {
    $.fn.selectRange = function(start, end) {
        if(!end) end = start;
        return this.each(function() {
            if (this.setSelectionRange) {
                this.focus();
                this.setSelectionRange(start, end);
            } else if (this.createTextRange) {
                var range = this.createTextRange();
                range.collapse(true);
                range.moveEnd('character', end);
                range.moveStart('character', start);
                range.select();
            }
        });
    };
})(jQuery);

// Extensions to standard javascript objects
String.prototype.escapeRegExp = function () {
    return this.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&");
}

// Custom functions
tinkerpop.namespace("tinkerpop.util");
tinkerpop.util = (function () {
    var _arrayToHexString = function (arr) {
        var result = "";
        for (i in arr) {
            var str = arr[i].toString(16);
            str = str.length == 0 ? "00" :
                  str.length == 1 ? "0" + str :
                  str.length == 2 ? str :
                  str.substring(str.length - 2, str.length);
            result += str;
        }

        return result;
    };

    return {
        arrayToHexString: _arrayToHexString,
        arrayToUuid: function (arr) {
            var hex = _arrayToHexString(arr);
            return hex.substring(0, 8) + "-" + hex.substring(8, 12) + "-" +
                   hex.substring(12, 16) + "-" + hex.substring(16, 20) + "-" +
                   hex.substring(20);
        },
        parseGrape: function (str, field) {
            var value = "";
            var start = str.indexOf(field + "=");
            if (start != -1) {
                var fragment = str.substr(start);
                var end = fragment.indexOf(",");

                if (end == -1) {
                    end = fragment.indexOf(")");
                }

                if (end != -1) {
                    value = fragment.substr(field.length + 1, end - field.length - 1);
                    value = value.replace(/'/g, "");
                }
            }

            return value;
        }
    };
}());
