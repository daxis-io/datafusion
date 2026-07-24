// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";

const fixtureUrl = new URL(
  "../../core/tests/data/tpch_region_small.parquet",
  import.meta.url,
);
const fixture = await readFile(fixtureUrl);
const etag = `"${createHash("sha256").update(fixture).digest("hex")}"`;
const port = Number.parseInt(process.env.PORT ?? "9876", 10);

function setCorsHeaders(response) {
  response.setHeader("Access-Control-Allow-Origin", "*");
  response.setHeader("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
  response.setHeader("Access-Control-Allow-Headers", "Range, If-Range");
  response.setHeader(
    "Access-Control-Expose-Headers",
    "Accept-Ranges, Content-Encoding, Content-Length, Content-Range, ETag",
  );
}

const server = createServer((request, response) => {
  setCorsHeaders(response);

  if (request.method === "OPTIONS") {
    response.writeHead(204);
    response.end();
    return;
  }

  if (request.url !== "/tpch_region_small.parquet") {
    response.writeHead(404);
    response.end();
    return;
  }

  response.setHeader("Accept-Ranges", "bytes");
  response.setHeader("Content-Encoding", "identity");
  response.setHeader("Content-Type", "application/vnd.apache.parquet");
  response.setHeader("ETag", etag);

  if (request.method === "HEAD") {
    response.setHeader("Content-Length", fixture.length);
    response.writeHead(200);
    response.end();
    return;
  }

  const range = request.headers.range;
  const ifRange = request.headers["if-range"];
  if (range && (!ifRange || ifRange === etag)) {
    const match = /^bytes=(\d+)-(\d+)$/.exec(range);
    if (!match) {
      response.writeHead(416);
      response.end();
      return;
    }

    const start = Number.parseInt(match[1], 10);
    const end = Math.min(Number.parseInt(match[2], 10), fixture.length - 1);
    if (start > end || start >= fixture.length) {
      response.setHeader("Content-Range", `bytes */${fixture.length}`);
      response.writeHead(416);
      response.end();
      return;
    }

    const body = fixture.subarray(start, end + 1);
    response.setHeader("Content-Length", body.length);
    response.setHeader(
      "Content-Range",
      `bytes ${start}-${end}/${fixture.length}`,
    );
    response.writeHead(206);
    response.end(body);
    return;
  }

  response.setHeader("Content-Length", fixture.length);
  response.writeHead(200);
  response.end(fixture);
});

server.listen(port, "127.0.0.1", () => {
  console.log(`browser parquet server listening on http://127.0.0.1:${port}`);
});
