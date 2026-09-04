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

test.describe.configure({ mode: "serial" });
test.setTimeout(300_000);

test.beforeEach(async ({ page }) => {
  await page.goto("/");
});

test("instantiates the generated WebAssembly module", async ({ page }) => {
  const result = await page.evaluate(async () => {
    await window.wasmReady;
    return "wasm-init:ok";
  });
  expect(result).toBe("wasm-init:ok");
});

test("parses the committed Axon fixtures", async ({ page }) => {
  const result = await page.evaluate(() => window.runFixtureParseQualification());
  expect(result).toBe("rows=6,4,3");
});

test("constructs browser session state", async ({ page }) => {
  const result = await page.evaluate(() => window.runContextQualification());
  expect(result).toBe("context:ok");
});

test("registers the committed Axon fixtures", async ({ page }) => {
  const result = await page.evaluate(() => window.runFixtureQualification());
  expect(result).toBe("fixtures=orders,customers,shipments");
});

test("produces an optimized logical plan", async ({ page }) => {
  const result = await page.evaluate(() => window.runLogicalQualification());
  expect(result.value).toBe("logical-fields=2");
  expect(result.heartbeats).toBeGreaterThan(0);
});

test("planning crosses the JavaScript event loop", async ({ page }) => {
  const result = await page.evaluate(() => window.runPlanningQualification());
  expect(result.value).toContain("physical-optimize:ok");
  expect(result.heartbeats).toBeGreaterThan(0);
});

test("executes the Axon corpus and runtime contracts", async ({ page }) => {
  const result = await page.evaluate(() => window.runExecutionQualification());
  expect(result).toContain("cases=18");
  expect(result).toContain("runtime-contracts=ok");
});

test("execution cooperates after each 128-item budget", async ({ page }) => {
  const result = await page.evaluate(() => window.runCooperationQualification());
  expect(result.value).toBe(257);
  expect(result.heartbeats).toBeGreaterThan(0);
});
