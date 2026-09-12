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

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let ctx = datafusion::prelude::SessionContext::new_with_config(
        datafusion::prelude::SessionConfig::new().with_target_partitions(1),
    );
    let result = datafusion_wasmtest::run_narrow_kernel_query(&ctx).await?;
    let oracle: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/narrow-query-oracle.json"))
            .unwrap();
    assert_eq!(result["schema"], oracle["schema"]);
    assert_eq!(result["rows"], oracle["rows"]);
    assert!(result["factory_calls"].as_u64().unwrap() > 0);
    assert!(result["data_range_reads"].as_u64().unwrap() > 0);
    assert_eq!(result["truncated_footer"], "rejected");
    println!("{result}");
    Ok(())
}
