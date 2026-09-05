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

//! Disk-disabled profile for [`DiskManager`].

use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::spill_file::{SpillFile, TempFileFactory};
use datafusion_common::error::RuntimeCapability;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use datafusion_common::error::UnsupportedRuntimeCapability;
use datafusion_common::{DataFusionError, Result};

pub const DEFAULT_MAX_TEMP_DIRECTORY_SIZE: u64 = 100 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_SPILL_MERGE_FAN_IN: usize = 0;

fn unsupported(capability: RuntimeCapability) -> DataFusionError {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        DataFusionError::External(Box::new(UnsupportedRuntimeCapability::browser(
            capability,
        )))
    }
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    {
        DataFusionError::ResourcesExhausted(format!(
            "runtime was compiled without the {capability} capability"
        ))
    }
}

/// Builder for a compile-time-disabled disk manager.
#[derive(Clone, Debug)]
pub struct DiskManagerBuilder {
    mode: DiskManagerMode,
    max_temp_directory_size: u64,
    max_spill_merge_fan_in: usize,
}

impl Default for DiskManagerBuilder {
    fn default() -> Self {
        Self {
            mode: DiskManagerMode::Disabled,
            max_temp_directory_size: DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
            max_spill_merge_fan_in: DEFAULT_MAX_SPILL_MERGE_FAN_IN,
        }
    }
}

impl DiskManagerBuilder {
    pub fn set_mode(&mut self, mode: DiskManagerMode) {
        self.mode = mode;
    }

    pub fn with_mode(mut self, mode: DiskManagerMode) -> Self {
        self.set_mode(mode);
        self
    }

    pub fn set_temp_file_factory(&mut self, factory: Arc<dyn TempFileFactory>) {
        self.mode = DiskManagerMode::Custom(factory);
    }

    pub fn with_temp_file_factory(mut self, factory: Arc<dyn TempFileFactory>) -> Self {
        self.set_temp_file_factory(factory);
        self
    }

    pub fn set_max_temp_directory_size(&mut self, value: u64) {
        self.max_temp_directory_size = value;
    }

    pub fn with_max_temp_directory_size(mut self, value: u64) -> Self {
        self.set_max_temp_directory_size(value);
        self
    }

    pub fn set_max_spill_merge_fan_in(&mut self, value: usize) {
        self.max_spill_merge_fan_in = value;
    }

    pub fn with_max_spill_merge_fan_in(mut self, value: usize) -> Self {
        self.set_max_spill_merge_fan_in(value);
        self
    }

    /// Build a disabled manager, rejecting every path-backed or custom disk mode
    /// before it can allocate a path or invoke a host callback.
    pub fn build(self) -> Result<DiskManager> {
        if !matches!(self.mode, DiskManagerMode::Disabled) {
            return Err(unsupported(RuntimeCapability::Disk));
        }
        Ok(DiskManager {
            max_temp_directory_size: AtomicU64::new(self.max_temp_directory_size),
            max_spill_merge_fan_in: AtomicUsize::new(self.max_spill_merge_fan_in),
        })
    }
}

/// Disk modes retained for cross-target configuration compatibility.
#[derive(Clone, Default)]
pub enum DiskManagerMode {
    #[default]
    OsTmpDirectory,
    Directories(Vec<PathBuf>),
    Custom(Arc<dyn TempFileFactory>),
    Disabled,
}

impl Debug for DiskManagerMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OsTmpDirectory => formatter.write_str("OsTmpDirectory"),
            Self::Directories(paths) => {
                formatter.debug_tuple("Directories").field(paths).finish()
            }
            Self::Custom(_) => formatter.write_str("Custom(Arc<dyn TempFileFactory>)"),
            Self::Disabled => formatter.write_str("Disabled"),
        }
    }
}

/// Disk-disabled manager. It never owns or allocates host paths.
pub struct DiskManager {
    max_temp_directory_size: AtomicU64,
    max_spill_merge_fan_in: AtomicUsize,
}

impl Debug for DiskManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiskManager")
            .field("enabled", &false)
            .field("max_temp_directory_size", &self.max_temp_directory_size())
            .field("max_spill_merge_fan_in", &self.max_spill_merge_fan_in())
            .finish()
    }
}

/// Information about current spill usage.
#[derive(Debug, Clone, Copy)]
pub struct SpillingProgress {
    pub current_bytes: u64,
    pub active_files_count: usize,
}

impl DiskManager {
    pub fn builder() -> DiskManagerBuilder {
        DiskManagerBuilder::default()
    }

    pub fn set_max_temp_directory_size(&self, value: u64) -> Result<()> {
        if value != 0 {
            return Err(unsupported(RuntimeCapability::Disk));
        }
        self.max_temp_directory_size.store(0, Ordering::Relaxed);
        Ok(())
    }

    #[deprecated(
        since = "54.0.0",
        note = "Use `set_max_temp_directory_size` directly, it now takes &self"
    )]
    pub fn set_arc_max_temp_directory_size(this: &Arc<Self>, value: u64) -> Result<()> {
        this.set_max_temp_directory_size(value)
    }

    pub fn with_max_temp_directory_size(self, value: u64) -> Result<Self> {
        self.set_max_temp_directory_size(value)?;
        Ok(self)
    }

    pub fn used_disk_space(&self) -> u64 {
        0
    }

    pub fn max_temp_directory_size(&self) -> u64 {
        self.max_temp_directory_size.load(Ordering::Relaxed)
    }

    pub fn set_max_spill_merge_fan_in(&self, value: usize) {
        self.max_spill_merge_fan_in.store(value, Ordering::Relaxed);
    }

    pub fn max_spill_merge_fan_in(&self) -> usize {
        self.max_spill_merge_fan_in.load(Ordering::Relaxed)
    }

    pub fn spilling_progress(&self) -> SpillingProgress {
        SpillingProgress {
            current_bytes: 0,
            active_files_count: 0,
        }
    }

    pub fn temp_dir_paths(&self) -> Vec<PathBuf> {
        Vec::new()
    }

    pub fn tmp_files_enabled(&self) -> bool {
        false
    }

    pub fn create_tmp_file(
        self: &Arc<Self>,
        _request_description: &str,
    ) -> Result<Arc<dyn SpillFile>> {
        Err(unsupported(RuntimeCapability::Spill))
    }
}
