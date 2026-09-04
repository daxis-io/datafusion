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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::common::error::{RuntimeCapability, UnsupportedRuntimeCapability};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::MemTable;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coop::CooperativeStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common_runtime::channel::mpsc;
use datafusion_common_runtime::{SpawnedTask, yield_now};
use futures::StreamExt;

use crate::axon_fixture::{fixture_batches, load_corpus, normalize_batches};
use crate::axon_provider::{AxonOrdersExec, AxonOrdersProvider, InvocationCounts};

const PLANNING_SQL: &str = "SELECT customer_tier, SUM(amount_cents) AS gross FROM orders WHERE status <> 'cancelled' GROUP BY customer_tier ORDER BY customer_tier";

pub fn qualify_fixture_parse() -> Result<String> {
    let (orders, customers, shipments) = fixture_batches()?;
    Ok(format!(
        "rows={},{},{}",
        orders.num_rows(),
        customers.num_rows(),
        shipments.num_rows()
    ))
}

pub fn qualify_context_construction() -> Result<String> {
    let _ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    Ok("context:ok".into())
}

pub async fn qualify_fixture_registration() -> Result<String> {
    let (_ctx, _) = make_context()?;
    Ok("fixtures=orders,customers,shipments".into())
}

pub async fn qualify_logical_planning() -> Result<String> {
    let (ctx, _) = make_context()?;
    let dataframe = ctx.sql(PLANNING_SQL).await?;
    Ok(format!(
        "logical-fields={}",
        dataframe.schema().fields().len()
    ))
}

pub async fn qualify_planning() -> Result<String> {
    let (ctx, counts) = make_context()?;
    let dataframe = ctx.sql(PLANNING_SQL).await?;
    let plan = dataframe.create_physical_plan().await?;
    if plan.properties().output_partitioning().partition_count() != 1 {
        return Err(DataFusionError::Plan(
            "browser physical plan must have exactly one output partition".into(),
        ));
    }
    if counts.scans() == 0 {
        return Err(DataFusionError::Plan(
            "custom Axon provider was not invoked while planning".into(),
        ));
    }
    Ok("sql,logical-plan,logical-optimize,physical-plan,physical-optimize:ok".into())
}

pub async fn qualify_execution() -> Result<String> {
    let (ctx, counts) = make_context()?;
    let corpus = load_corpus()?;

    for case in &corpus {
        let dataframe = ctx.sql(&case.sql).await?;
        let plan = dataframe.create_physical_plan().await?;
        let actual_columns = plan
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        if actual_columns != case.expected_columns {
            return Err(DataFusionError::Execution(format!(
                "{} columns differ: expected {:?}, got {:?}",
                case.name, case.expected_columns, actual_columns
            )));
        }
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
        let rows = normalize_batches(&batches)?;
        if rows != case.expected_rows {
            return Err(DataFusionError::Execution(format!(
                "{} rows differ: expected {:?}, got {:?}",
                case.name, case.expected_rows, rows
            )));
        }
    }

    if counts.scans() < corpus.len() || counts.executions() < corpus.len() {
        return Err(DataFusionError::Execution(format!(
            "custom provider/plan was not used for every case: scans={}, executions={}, cases={}",
            counts.scans(),
            counts.executions(),
            corpus.len()
        )));
    }

    prove_nonzero_partition_rejected(&ctx, &counts)?;
    prove_disk_and_spill_errors()?;
    prove_bounded_channel().await?;
    prove_drop_cancellation().await?;
    prove_memory_pool()?;

    Ok(format!(
        "cases=18;custom_scans={};custom_executes={};partition_rejections={};runtime-contracts=ok",
        counts.scans(),
        counts.executions(),
        counts.rejected_partitions()
    ))
}

pub async fn qualify_cooperation() -> Result<u32> {
    let (orders, _, _) = fixture_batches()?;
    let stream = MemoryStream::try_new(
        std::iter::repeat_n(orders.clone(), 257).collect(),
        orders.schema(),
        None,
    )?;
    let mut stream = CooperativeStream::new(stream);
    let mut count = 0_u32;
    while let Some(batch) = stream.next().await {
        batch?;
        count += 1;
    }
    if count != 257 {
        return Err(DataFusionError::Execution(format!(
            "cooperative stream produced {count} batches, expected 257"
        )));
    }
    Ok(count)
}

fn make_context() -> Result<(SessionContext, Arc<InvocationCounts>)> {
    let (orders, customers, shipments) = fixture_batches()?;
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    let counts = Arc::new(InvocationCounts::default());
    ctx.register_table(
        "orders",
        Arc::new(AxonOrdersProvider::new(orders, Arc::clone(&counts))),
    )?;
    ctx.register_table(
        "customers",
        Arc::new(MemTable::try_new(
            customers.schema(),
            vec![vec![customers]],
        )?),
    )?;
    ctx.register_table(
        "shipments",
        Arc::new(MemTable::try_new(
            shipments.schema(),
            vec![vec![shipments]],
        )?),
    )?;
    Ok((ctx, counts))
}

fn prove_nonzero_partition_rejected(
    ctx: &SessionContext,
    counts: &Arc<InvocationCounts>,
) -> Result<()> {
    let (orders, _, _) = fixture_batches()?;
    let plan = AxonOrdersExec::new(orders, Arc::clone(counts));
    let error = match plan.execute(1, ctx.task_ctx()) {
        Ok(_) => {
            return Err(DataFusionError::Execution(
                "nonzero partition unexpectedly executed".into(),
            ));
        }
        Err(error) => error,
    };
    if !error.to_string().contains("only supports partition 0") {
        return Err(DataFusionError::Execution(format!(
            "nonzero partition returned unexpected error: {error}"
        )));
    }
    Ok(())
}

fn prove_disk_and_spill_errors() -> Result<()> {
    let disk_error = DiskManagerBuilder::default()
        .with_mode(DiskManagerMode::OsTmpDirectory)
        .build()
        .expect_err("browser OS temporary storage must be rejected");
    assert_capability(&disk_error, RuntimeCapability::Disk)?;

    let manager = Arc::new(
        DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::Disabled)
            .build()?,
    );
    let spill_error = match manager.create_tmp_file("browser qualification") {
        Ok(_) => {
            return Err(DataFusionError::Execution(
                "browser spill unexpectedly created a temporary file".into(),
            ));
        }
        Err(error) => error,
    };
    assert_capability(&spill_error, RuntimeCapability::Spill)
}

fn assert_capability(error: &DataFusionError, expected: RuntimeCapability) -> Result<()> {
    let DataFusionError::External(source) = error else {
        return Err(DataFusionError::Execution(format!(
            "runtime capability error was not External: {error}"
        )));
    };
    let typed = source
        .downcast_ref::<UnsupportedRuntimeCapability>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "runtime capability error lost its typed source: {error}"
            ))
        })?;
    if typed.capability != expected || typed.profile.to_string() != "browser" {
        return Err(DataFusionError::Execution(format!(
            "unexpected runtime capability error: {typed}"
        )));
    }
    Ok(())
}

async fn prove_bounded_channel() -> Result<()> {
    let (sender, mut receiver) = mpsc::channel(2);
    sender.try_send(1).map_err(external)?;
    sender.try_send(2).map_err(external)?;
    if receiver.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "bounded channel high-water mark was {}, expected 2",
            receiver.len()
        )));
    }
    let full = sender
        .try_send(3)
        .expect_err("third send must exceed global capacity");
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let is_full = full.is_full();
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    let is_full = matches!(full, mpsc::error::TrySendError::Full(_));
    if !is_full || full.into_inner() != 3 {
        return Err(DataFusionError::Execution(
            "bounded channel did not preserve the full-send value".into(),
        ));
    }
    if receiver.recv().await != Some(1) {
        return Err(DataFusionError::Execution(
            "bounded channel did not preserve FIFO ordering".into(),
        ));
    }
    sender.try_send(3).map_err(external)?;
    if receiver.len() != 2 {
        return Err(DataFusionError::Execution(
            "bounded channel did not return the consumed credit".into(),
        ));
    }
    receiver.close();
    if !sender.is_closed() {
        return Err(DataFusionError::Execution(
            "bounded sender did not observe receiver closure".into(),
        ));
    }
    Ok(())
}

async fn prove_drop_cancellation() -> Result<()> {
    struct DropProbe(Arc<AtomicBool>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let published = Arc::new(AtomicBool::new(false));
    let dropped_in_task = Arc::clone(&dropped);
    let published_in_task = Arc::clone(&published);
    let future = async move {
        let _probe = DropProbe(dropped_in_task);
        futures::future::pending::<()>().await;
        published_in_task.store(true, Ordering::SeqCst);
    };
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let task = SpawnedTask::spawn_local(future);
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    let task = SpawnedTask::spawn(future);
    yield_now().await;
    drop(task);
    yield_now().await;
    if !dropped.load(Ordering::SeqCst) || published.load(Ordering::SeqCst) {
        return Err(DataFusionError::Execution(
            "task drop did not cancel cleanly before late publication".into(),
        ));
    }
    Ok(())
}

fn prove_memory_pool() -> Result<()> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8));
    let reservation = MemoryConsumer::new("browser qualification").register(&pool);
    reservation.try_grow(8)?;
    if reservation.try_grow(1).is_ok() {
        return Err(DataFusionError::Execution(
            "memory pool allowed a reservation above its limit".into(),
        ));
    }
    drop(reservation);
    if pool.reserved() != 0 {
        return Err(DataFusionError::Execution(
            "memory reservation was not released on drop".into(),
        ));
    }
    Ok(())
}

fn external(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
