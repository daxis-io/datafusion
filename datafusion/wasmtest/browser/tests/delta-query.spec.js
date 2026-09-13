import { readFileSync } from "node:fs";
import { expect, test } from "@playwright/test";

test.setTimeout(300_000);
test("Delta adapter real query and negatives match native", async ({ page }, testInfo) => {
  const native = JSON.parse(readFileSync(process.env.DELTA_NATIVE_RESULT, "utf8"));
  await page.goto("/");
  await page.evaluate(() => window.wasmReady);
  const result = await page.evaluate(async () => {
    const wasm = await import("/pkg/datafusion_wasmtest.js");
    await wasm.default();
    return JSON.parse(await wasm.qualify_delta_query());
  });
  expect(result).toEqual(native);
  expect(result.latest.rows).toHaveLength(4);
  expect(result.version0.rows).toHaveLength(8);
  expect(result.missing_reader).toBe("rejected_before_io");
  expect(result.truncated_active_parquet).toBe("rejected");
  expect(result.corrupt_active_parquet).toBe("rejected");
  await testInfo.attach("delta-browser-result.json", {body:JSON.stringify(result,null,2),contentType:"application/json"});
});
