// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
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

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { once } from "node:events";
import test from "node:test";

async function unusedLoopbackPort() {
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const { port } = server.address();
  server.close();
  await once(server, "close");
  return port;
}

test("preflight permits the headers used by browser Fetch adapters", async () => {
  const port = await unusedLoopbackPort();
  const server = spawn(process.execPath, ["browser-server.mjs"], {
    cwd: new URL(".", import.meta.url),
    env: { ...process.env, PORT: String(port) },
    stdio: ["ignore", "pipe", "pipe"],
  });

  try {
    await once(server.stdout, "data");
    const response = await fetch(
      `http://127.0.0.1:${port}/tpch_region_small.parquet`,
      {
        method: "OPTIONS",
        headers: {
          Origin: "http://127.0.0.1:12345",
          "Access-Control-Request-Headers": "range,if-range,user-agent",
          "Access-Control-Request-Method": "HEAD",
        },
      },
    );

    assert.equal(response.status, 204);
    assert.match(
      response.headers.get("access-control-allow-headers") ?? "",
      /(?:^|,\s*)User-Agent(?:,|$)/,
    );
  } finally {
    server.kill();
    await once(server, "exit");
  }
});
