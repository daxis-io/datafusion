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

import { readFileSync } from "node:fs";
import { expect, test } from "@playwright/test";

test.skip(process.env.DATAFUSION_NARROW_QUERY !== "1", "requires narrow query build");
test.setTimeout(300_000);

test("Kernel converters filter and project real Parquet in Chromium", async ({ page }, testInfo) => {
  const oracle = JSON.parse(readFileSync(new URL("../../fixtures/narrow-query-oracle.json", import.meta.url)));
  await page.goto("/");
  await page.evaluate(() => window.wasmReady);
  const result = await page.evaluate(async () => {
    const wasm = await import("/pkg/datafusion_wasmtest.js");
    if (typeof wasm.qualify_narrow_kernel_query !== "function") {
      throw new Error("narrow Kernel query export is absent");
    }
    await wasm.default();
    return JSON.parse(await wasm.qualify_narrow_kernel_query());
  });
  expect(result.schema).toEqual(oracle.schema);
  expect(result.rows).toEqual(oracle.rows);
  expect(result.fixture_sha256).toBe("d1cacf639ebbd275ca08c448fcd9e97cec3a1098b45086cd33635a6a0113c462");
  expect(result.factory_calls).toBeGreaterThan(0);
  expect(result.data_range_reads).toBeGreaterThan(0);
  expect(result.truncated_footer).toBe("rejected");
  const native = JSON.parse(readFileSync(process.env.NARROW_NATIVE_RESULT, "utf8"));
  expect(result).toEqual(native);
  await testInfo.attach("narrow-browser-result.json", {
    body: JSON.stringify(result, null, 2),
    contentType: "application/json",
  });
});
