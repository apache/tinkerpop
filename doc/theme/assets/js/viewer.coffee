$ ->
    if hljs?
        $('#content pre code').each (i, el) ->
            hljs.highlightBlock el

    if window.location.search.indexOf('print') != -1
        window.print()
