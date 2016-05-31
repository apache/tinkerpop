/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
@Grab(group = 'com.opencsv', module = 'opencsv', version = '3.7')
import com.opencsv.*

import org.apache.tinkerpop.gremlin.process.computer.bulkdumping.BulkExportVertexProgram

def stringify(vertex) {
  def result = null
  def rows = vertex.property(BulkExportVertexProgram.BULK_EXPORT_PROPERTIES)
  if (rows.isPresent()) {
    def writer = new StringWriter()
    def w = new CSVWriter(writer)
    rows.value().each { def row ->
      w.writeNext((String[]) row, false)
    }
    result = writer.toString().trim()
  }
  return result
}
