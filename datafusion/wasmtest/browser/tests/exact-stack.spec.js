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

import { expect, test } from "@playwright/test";

test.skip(
  process.env.DATAFUSION_EXACT_STACK !== "1",
  "requires the ignored exact-stack overlay build"
);
test.setTimeout(300_000);

test("qualifies custom Parquet reads from the exact local stack", async ({
  page,
}) => {
  page.on("console", (message) => console.log(`browser:${message.text()}`));
  await page.goto("/?profile=exact");
  await page.evaluate(() => window.wasmReady);
  expect(await page.evaluate(() => window.qualificationProfile)).toBe("exact");
  const result = await page.evaluate(() => window.runExactStackQualification());
  expect(result).toContain("uncompressed_ranges=");
  expect(result).toContain("zstd_ranges=");
  expect(result).toContain("checksum,range,footer,zstd,cancellation=ok");
});
