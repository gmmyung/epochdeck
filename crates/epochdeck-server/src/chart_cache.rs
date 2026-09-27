//! Bounded, revision-scoped caches for exact chart extents and aggregate series.

use std::collections::HashMap;

use epochdeck_protocol::{ChartAlignment, ChartMetricHistory, RunId};
use epochdeck_storage::ChartAxisExtent;

const CHART_SERIES_CACHE_MAX_ENTRIES: usize = 512;
const CHART_SERIES_CACHE_MAX_CELLS: usize = 250_000;
const CHART_SERIES_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;
const CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES: usize = 2_048;
const CHART_AXIS_EXTENT_CACHE_MAX_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ChartAxisExtentCacheKey {
    pub(super) run_id: RunId,
    pub(super) key: String,
    pub(super) source_first_sequence: u64,
    pub(super) source_last_sequence: u64,
}

#[derive(Debug, Clone)]
struct CachedChartAxisExtent {
    extent: Option<ChartAxisExtent>,
    bytes: usize,
    last_used: u64,
}

#[derive(Debug, Default)]
pub(super) struct ChartAxisExtentCache {
    entries: HashMap<ChartAxisExtentCacheKey, CachedChartAxisExtent>,
    bytes: usize,
    clock: u64,
    #[cfg(test)]
    scans: u64,
}

impl ChartAxisExtentCache {
    pub(super) fn get(&mut self, key: &ChartAxisExtentCacheKey) -> Option<Option<ChartAxisExtent>> {
        self.clock = self.clock.saturating_add(1);
        let entry = self.entries.get_mut(key)?;
        entry.last_used = self.clock;
        Some(entry.extent)
    }

    pub(super) fn insert(&mut self, key: ChartAxisExtentCacheKey, extent: Option<ChartAxisExtent>) {
        let bytes = std::mem::size_of::<ChartAxisExtentCacheKey>()
            .saturating_add(std::mem::size_of::<CachedChartAxisExtent>())
            .saturating_add(key.key.capacity());
        if bytes > CHART_AXIS_EXTENT_CACHE_MAX_BYTES {
            return;
        }
        self.remove(&key);
        self.clock = self.clock.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.insert(
            key,
            CachedChartAxisExtent {
                extent,
                bytes,
                last_used: self.clock,
            },
        );
        while self.entries.len() > CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES
            || self.bytes > CHART_AXIS_EXTENT_CACHE_MAX_BYTES
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.remove(&oldest);
        }
    }

    fn remove(&mut self, key: &ChartAxisExtentCacheKey) {
        if let Some(removed) = self.entries.remove(key) {
            self.bytes = self.bytes.saturating_sub(removed.bytes);
        }
    }

    #[cfg(test)]
    pub(super) fn record_scan(&mut self) {
        self.scans = self.scans.saturating_add(1);
    }

    #[cfg(test)]
    pub(super) fn scan_count(&self) -> u64 {
        self.scans
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum CachedChartOrigin {
    Step,
    RelativeStep(u64),
    ElapsedTime(i64),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ChartSeriesCacheKey {
    pub(super) run_id: RunId,
    pub(super) key: String,
    pub(super) source_last_sequence: u64,
    pub(super) alignment: ChartAlignment,
    pub(super) origin: CachedChartOrigin,
    pub(super) x_min: u64,
    pub(super) x_max: u64,
    pub(super) max_buckets: usize,
}

#[derive(Debug, Clone)]
struct CachedChartSeries {
    history: ChartMetricHistory,
    cells: usize,
    bytes: usize,
    last_used: u64,
}

#[derive(Debug, Default)]
pub(super) struct ChartSeriesCache {
    entries: HashMap<ChartSeriesCacheKey, CachedChartSeries>,
    cells: usize,
    bytes: usize,
    clock: u64,
}

impl ChartSeriesCache {
    pub(super) fn get(&mut self, key: &ChartSeriesCacheKey) -> Option<ChartMetricHistory> {
        self.clock = self.clock.saturating_add(1);
        let entry = self.entries.get_mut(key)?;
        entry.last_used = self.clock;
        Some(entry.history.clone())
    }

    pub(super) fn insert(&mut self, key: ChartSeriesCacheKey, history: ChartMetricHistory) {
        let cells = history.bucket.len();
        let bytes = chart_series_cache_bytes(&key, &history);
        if cells > CHART_SERIES_CACHE_MAX_CELLS || bytes > CHART_SERIES_CACHE_MAX_BYTES {
            return;
        }
        self.remove(&key);
        self.clock = self.clock.saturating_add(1);
        self.cells = self.cells.saturating_add(cells);
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.insert(
            key,
            CachedChartSeries {
                history,
                cells,
                bytes,
                last_used: self.clock,
            },
        );
        while self.entries.len() > CHART_SERIES_CACHE_MAX_ENTRIES
            || self.cells > CHART_SERIES_CACHE_MAX_CELLS
            || self.bytes > CHART_SERIES_CACHE_MAX_BYTES
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.remove(&oldest);
        }
    }

    fn remove(&mut self, key: &ChartSeriesCacheKey) {
        if let Some(removed) = self.entries.remove(key) {
            self.cells = self.cells.saturating_sub(removed.cells);
            self.bytes = self.bytes.saturating_sub(removed.bytes);
        }
    }
}

fn chart_series_cache_bytes(key: &ChartSeriesCacheKey, history: &ChartMetricHistory) -> usize {
    std::mem::size_of::<ChartSeriesCacheKey>()
        .saturating_add(std::mem::size_of::<CachedChartSeries>())
        .saturating_add(key.key.capacity())
        .saturating_add(
            history
                .bucket
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        )
        .saturating_add(
            history
                .last_x
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>()),
        )
        .saturating_add(
            history
                .last_step
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>()),
        )
        .saturating_add(
            history
                .last_timestamp_ms
                .capacity()
                .saturating_mul(std::mem::size_of::<i64>()),
        )
        .saturating_add(
            history
                .minimum
                .capacity()
                .saturating_mul(std::mem::size_of::<f64>()),
        )
        .saturating_add(
            history
                .maximum
                .capacity()
                .saturating_mul(std::mem::size_of::<f64>()),
        )
        .saturating_add(
            history
                .last
                .capacity()
                .saturating_mul(std::mem::size_of::<f64>()),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chart_axis_extent_cache_is_bounded_and_caches_missing_metrics() {
        let run_id = RunId::new();
        let keys = (0..=CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES)
            .map(|index| ChartAxisExtentCacheKey {
                run_id,
                key: format!("metric-{index}"),
                source_first_sequence: 1,
                source_last_sequence: 10,
            })
            .collect::<Vec<_>>();
        let mut cache = ChartAxisExtentCache::default();
        for (index, key) in keys
            .iter()
            .take(CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES)
            .enumerate()
        {
            cache.insert(
                key.clone(),
                (index != 0).then_some(ChartAxisExtent {
                    step_minimum: index as u64,
                    step_maximum: index as u64 + 1,
                    timestamp_minimum_ms: index as i64,
                    timestamp_maximum_ms: index as i64 + 1,
                }),
            );
        }
        assert_eq!(cache.get(&keys[0]), Some(None));
        cache.insert(
            keys[CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES].clone(),
            Some(ChartAxisExtent {
                step_minimum: 0,
                step_maximum: 1,
                timestamp_minimum_ms: 0,
                timestamp_maximum_ms: 1,
            }),
        );
        assert!(cache.get(&keys[0]).is_some());
        assert!(cache.get(&keys[1]).is_none());
        assert!(cache.entries.len() <= CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES);
    }

    #[test]
    fn chart_series_cache_is_bounded_and_updates_recency() {
        let mut cache = ChartSeriesCache::default();
        let keys = (0..=CHART_SERIES_CACHE_MAX_ENTRIES)
            .map(|index| ChartSeriesCacheKey {
                run_id: RunId::new(),
                key: format!("metric-{index}"),
                source_last_sequence: 1,
                alignment: ChartAlignment::Step,
                origin: CachedChartOrigin::Step,
                x_min: 0,
                x_max: 1,
                max_buckets: 2,
            })
            .collect::<Vec<_>>();
        for key in keys.iter().take(CHART_SERIES_CACHE_MAX_ENTRIES) {
            cache.insert(key.clone(), ChartMetricHistory::default());
        }
        assert!(cache.get(&keys[0]).is_some());
        cache.insert(
            keys[CHART_SERIES_CACHE_MAX_ENTRIES].clone(),
            ChartMetricHistory::default(),
        );

        assert_eq!(cache.entries.len(), CHART_SERIES_CACHE_MAX_ENTRIES);
        assert!(cache.entries.contains_key(&keys[0]));
        assert!(!cache.entries.contains_key(&keys[1]));
        assert!(
            cache
                .entries
                .contains_key(&keys[CHART_SERIES_CACHE_MAX_ENTRIES])
        );
    }
}
