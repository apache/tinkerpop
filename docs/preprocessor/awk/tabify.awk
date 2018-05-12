# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#
# @author Daniel Kuppitz (http://gremlin.guru)
#
function print_tabs(next_id, tabs, blocks) {

  num_tabs = length(tabs)
  x = next_id

  print "++++"
  print "<section class=\"tabs tabs-" num_tabs "\">"

  for (i in tabs) {
    title = tabs[i]
    print "  <input id=\"tab-" id_part "-" x "\" type=\"radio\" name=\"radio-set-" id_part "-" next_id "\" class=\"tab-selector-" i "\"" (i == 1 ? " checked=\"checked\"" : "") " />"
    print "  <label for=\"tab-" id_part "-" x "\" class=\"tab-label-" i "\">" title "</label>"
    x++
  }

  for (i in blocks) {
    print "  <div class=\"tabcontent\">"
    print "    <div class=\"tabcontent-" i "\">"
    print "++++\n"
    print blocks[i]
    print "++++"
    print "    </div>"
    print "  </div>"
  }

  print "</section>"
  print "++++\n"
}

function transform_callouts(code, c) {
  return gensub(/\s*((<[0-9]+>\s*)*<[0-9]+>)\s*\n/, " " c c " \\1\\2\n", "g", code)
}

BEGIN {
  id_part=systime()
  status = 0
  next_id = 1
  block[0] = 0 # initialize "blocks" as an array
  delete blocks[0]
}

/^\[gremlin-/ {
  status = 1
  lang = gensub(/^\[gremlin-([^,\]]+).*/, "\\1", "g", $0)
  code = ""
}

/^\[source,(csharp|groovy|java|python)/ {
  if (status == 3) {
    status = 1
    lang = gensub(/^\[source,([^\]]+).*/, "\\1", "g", $0)
    code = ""
  }
}

! /^\[source,(csharp|groovy|java|python)/ {
  if (status == 3 && $0 != "") {
    print_tabs(next_id, tabs, blocks)
    next_id = next_id + length(tabs)
    for (i in tabs) {
      delete tabs[i]
      delete blocks[i]
    }
    status = 0
  }
}

/^----$/ {
  if (status == 1) {
    status = 2
  } else if (status == 2) {
    status = 3
  }
}

{ if (status == 3) {
    if ($0 == "----") {
      i = length(blocks) + 1
      if (i == 1) {
        tabs[i] = "console (" lang ")"
        blocks[i] = code_header code "\n" $0 "\n"
        i++
      }
      tabs[i] = lang
      switch (lang) {
        case "python":
          c = "#"
          break
        default:
          c = "//"
          break
      }
      blocks[i] = "[source," lang "]" transform_callouts(code, c) "\n" $0 "\n"
    }
  } else {
    if (status == 0) print
    else if (status == 1) code_header = $0
    else code = code "\n" $0
  }
}
