#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

BEGIN {
  firstMatch=1
  styled=0
}

/Licensed to the Apache Software Foundation/ {
  isHeader=1
}

/<\/style>/ {
  if (!styled) {
    print ".invisible {color: rgba(0,0,0,0); font-size: 0;}"
    styled=1
  }
}

!/<span class="comment">/ {
  if (firstMatch || !isHeader) {
    print gensub(/(<b class="conum">)\(([0-9]+)\)(<\/b>)/,
                 "<span class=\"invisible\">//</span>\\1\\2\\3", "g")
  }
}

/<span class="comment">/ {
  if (firstMatch || !isHeader) {
    print gensub(/<span class="comment">\/\/<\/span>(<b class="conum">)\(([0-9]+)\)(<\/b>)/,
                 "<span class=\"invisible\">//</span>\\1\\2\\3<span class=\"invisible\">\\\\<\/span>", "g")
  }
}

/under the License\./ {
  firstMatch=0
  isHeader=0
}
