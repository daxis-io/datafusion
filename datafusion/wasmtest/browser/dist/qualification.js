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

let wasm;
window.wasmReady = (async () => {
  const profile =
    new URLSearchParams(window.location.search).get("profile") ?? "ordinary";
  if (profile !== "ordinary" && profile !== "exact") {
    throw new Error(`Unknown qualification profile: ${profile}`);
  }
  wasm = await import(
    profile === "exact"
      ? "./pkg/datafusion_browser_exact_stack.js"
      : "./pkg/datafusion_wasmtest.js"
  );
  await wasm.default();
  // Report the loaded artifact's export, so a wrong module cannot pass merely
  // by echoing the requested profile.
  window.qualificationProfile = wasm.qualify_exact_stack ? "exact" : "ordinary";
})();
const ready = window.wasmReady;

async function withHeartbeat(operation) {
  let heartbeats = 0;
  const timer = setInterval(() => {
    heartbeats += 1;
  }, 0);
  try {
    const value = await operation();
    return { value, heartbeats };
  } finally {
    clearInterval(timer);
  }
}

window.runQualification = async () => {
  window.qualificationStage = "wasm-init";
  await ready;
  window.qualificationStage = "planning";
  const planning = await withHeartbeat(() => wasm.qualify_planning());
  window.qualificationStage = "execution";
  const execution = await wasm.qualify_execution();
  window.qualificationStage = "cooperation";
  const cooperation = await withHeartbeat(() => wasm.qualify_cooperation());
  window.qualificationStage = "exact-stack";
  const exactStack = wasm.qualify_exact_stack_parquet
    ? await wasm.qualify_exact_stack_parquet()
    : "not-enabled";
  window.qualificationStage = "complete";
  return {
    planning,
    execution,
    cooperation,
    exactStack,
  };
};

window.runPlanningQualification = async () => {
  await ready;
  return withHeartbeat(() => wasm.qualify_planning());
};

window.runFixtureQualification = async () => {
  await ready;
  return wasm.qualify_fixture_registration();
};

window.runFixtureParseQualification = async () => {
  await ready;
  return wasm.qualify_fixture_parse();
};

window.runContextQualification = async () => {
  await ready;
  return wasm.qualify_context_construction();
};

window.runLogicalQualification = async () => {
  await ready;
  return withHeartbeat(() => wasm.qualify_logical_planning());
};

window.runExecutionQualification = async () => {
  await ready;
  return wasm.qualify_execution();
};

window.runCooperationQualification = async () => {
  await ready;
  return withHeartbeat(() => wasm.qualify_cooperation());
};

window.runExactStackQualification = async () => {
  await ready;
  return wasm.qualify_exact_stack();
};
