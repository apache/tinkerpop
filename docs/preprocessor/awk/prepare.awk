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
BEGIN {
  p = 0
}

function escape_string(string) {
  str = gensub(/\\/, "\\\\\\\\", "g", string)
  return gensub(/'/, "\\\\'", "g", str)
}

function print_string(string) {
  print "pb(" p++ "); '" escape_string(string) "'"
}

function transform_callouts(code) {
  return gensub(/\s*((<[0-9]+>\s*)*<[0-9]+>)\s*$/, " //// \\1", "g", code)
}

function remove_callouts(code) {
  return gensub(/\s*((<[0-9]+>\s*)*<[0-9]+>)\s*$/, "", "g", code)
}

/^----$/ {
  if (inCodeSection) {
    if (prepared) {
      inCodeSection = 0
      prepared = 0
    } else {
      prepared = 1
    }
  }
  print_string($0)
}

!/^----$/ {
  if (inCodeSection) {
    if ($0 ~ /^:/) {
      print "'" escape_string(transform_callouts($0)) "'"
      print remove_callouts($0)
    } else {
      print transform_callouts($0)
    }
  } else {
    print_string($0)
  }
}

/^\[gremlin-/ {
  inCodeSection = 1
}

END {
  print_string("// LAST LINE")
}
