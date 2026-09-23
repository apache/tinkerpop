/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

// Static server for .generated/ bundle and fixture (Playwright webServer)
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const generatedDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '.generated');
const port = Number(process.env.PORT ?? 4173);

const contentTypes = { '.html': 'text/html', '.js': 'text/javascript' };

const server = http.createServer((req, res) => {
  const urlPath = req.url === '/' ? '/fixture.html' : req.url;
  const filePath = path.join(generatedDir, urlPath);

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': contentTypes[path.extname(filePath)] ?? 'application/octet-stream' });
    res.end(data);
  });
});

server.listen(port, () => {
  console.log(`Serving ${generatedDir} on http://localhost:${port}`);
});
