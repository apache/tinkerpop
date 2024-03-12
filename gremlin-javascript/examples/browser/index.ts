/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import Color from "colorjs.io";
import cytoscape from "cytoscape";
import * as gremlin from "gremlin";
import type { Edge, Vertex } from "gremlin/build/esm/structure/graph";
import { createRandomColorGenerator } from "./utils";

const randomColor = createRandomColorGenerator();

const g = gremlin.process.AnonymousTraversalSource.traversal().with_(
  new gremlin.driver.DriverRemoteConnection("ws://localhost:8182/gremlin")
);

const [vertices, edges] = await Promise.all([
  await g.V().toList<Vertex>(),
  await g.E().toList<Edge>(),
]);

cytoscape({
  container: globalThis.document.getElementById("graph"),
  elements: [
    ...vertices.map((vertex) => ({
      data: {
        id: vertex.id,
      },
      style: {
        label: `${vertex.label}\n${vertex.id}`,
        "background-color": randomColor(vertex.label),
        "border-color": new Color(randomColor(vertex.label)).darken().toString(
          // @ts-expect-error
          {
            format: "hex",
          }
        ),
      },
    })),
    ...edges.map((edge) => ({
      data: {
        id: edge.id,
        label: edge.label,
        source: edge.inV.id,
        target: edge.outV.id,
      },
    })),
  ],
  style: [
    {
      selector: "node",
      style: {
        color: "white",
        "text-valign": "center",
        "text-halign": "center",
        "border-width": 4,
        width: "80px",
        height: "80px",
        "text-max-width": "80px",
        "text-wrap": "wrap",
      },
    },

    {
      selector: "edge",
      style: {
        label: "data(label)",
        color: "white",
        "text-outline-color": "black",
        "text-outline-width": "2px",
        "text-margin-y": "-6px",
        "text-rotation": "autorotate",
        "target-arrow-shape": "triangle",
        "curve-style": "bezier",
      },
    },
  ],
  layout: {
    name: "cose",
  },
});
