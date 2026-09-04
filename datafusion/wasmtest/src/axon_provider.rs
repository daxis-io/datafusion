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

use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, SchedulingType,
};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

#[derive(Debug, Default)]
pub struct InvocationCounts {
    scans: AtomicUsize,
    executions: AtomicUsize,
    rejected_partitions: AtomicUsize,
}

impl InvocationCounts {
    pub fn scans(&self) -> usize {
        self.scans.load(Ordering::SeqCst)
    }

    pub fn executions(&self) -> usize {
        self.executions.load(Ordering::SeqCst)
    }

    pub fn rejected_partitions(&self) -> usize {
        self.rejected_partitions.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct AxonOrdersProvider {
    batch: RecordBatch,
    counts: Arc<InvocationCounts>,
}

impl AxonOrdersProvider {
    pub fn new(batch: RecordBatch, counts: Arc<InvocationCounts>) -> Self {
        Self { batch, counts }
    }
}

#[async_trait]
impl TableProvider for AxonOrdersProvider {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.counts.scans.fetch_add(1, Ordering::SeqCst);
        let batch = match projection {
            Some(indices) => self.batch.project(indices)?,
            None => self.batch.clone(),
        };
        Ok(Arc::new(AxonOrdersExec::new(
            batch,
            Arc::clone(&self.counts),
        )))
    }
}

#[derive(Debug)]
pub struct AxonOrdersExec {
    batch: RecordBatch,
    properties: Arc<PlanProperties>,
    counts: Arc<InvocationCounts>,
}

impl AxonOrdersExec {
    pub fn new(batch: RecordBatch, counts: Arc<InvocationCounts>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(batch.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);
        Self {
            batch,
            properties: Arc::new(properties),
            counts,
        }
    }
}

impl DisplayAs for AxonOrdersExec {
    fn fmt_as(
        &self,
        format: DisplayFormatType,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        match format {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AxonOrdersExec: partitions=1")
            }
            DisplayFormatType::TreeRender => write!(f, "AxonOrdersExec"),
        }
    }
}

impl ExecutionPlan for AxonOrdersExec {
    fn name(&self) -> &'static str {
        "AxonOrdersExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return exec_err!("AxonOrdersExec cannot accept children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            self.counts
                .rejected_partitions
                .fetch_add(1, Ordering::SeqCst);
            return exec_err!(
                "AxonOrdersExec only supports partition 0; received {partition}"
            );
        }
        self.counts.executions.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(MemoryStream::try_new(
            vec![self.batch.clone()],
            self.batch.schema(),
            None,
        )?))
    }
}
