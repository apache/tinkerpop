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
  def haltedTraversers = vertex.property(TraversalVertexProgram.HALTED_TRAVERSERS)
  if (haltedTraversers.isPresent()) {
    def properties = vertex.value(BulkExportVertexProgram.BULK_EXPORT_PROPERTIES).split("\1")*.split("\2", 2)*.toList()
    def writer = new StringWriter()
    def w = new CSVWriter(writer)
    haltedTraversers.value().each { def t ->
      def values = []
      properties.each { def property, def format ->
        def value = t.path(property)
        values << (format.isEmpty() ? value.toString() : String.format(format, value))
      }
      w.writeNext((String[]) values, false)
    }
    result = writer.toString().trim()
    writer.close()
  }
  return result
}
