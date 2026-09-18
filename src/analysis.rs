use crate::{
    config::{ArgsCommitment, Endpoint, EndpointKind},
    utils::{Comparator, TransactionData, percentile},
};
use comfy_table::{ContentArrangement, Table};
use serde_json::{Map, Value, json};
use std::cmp::Ordering;

#[cfg(target_os = "windows")]
#[inline]
fn table_preset() -> &'static str {
    comfy_table::presets::ASCII_FULL
}

#[cfg(not(target_os = "windows"))]
#[inline]
fn table_preset() -> &'static str {
    comfy_table::presets::UTF8_FULL
}
use std::collections::HashMap;
use std::time::Duration;

#[derive(Default)]
pub struct EndpointStats {
    pub total_observations: usize,
    pub first_detections: usize,
    pub delays_ms: Vec<f64>,
    pub backfill_transactions: usize,
}

#[derive(Debug, Default, Clone)]
pub struct EndpointSummary {
    pub name: String,
    pub first_share: f64,
    pub p50_delay_ms: Option<f64>,
    pub p95_delay_ms: Option<f64>,
    pub p99_delay_ms: Option<f64>,
    pub valid_transactions: usize,
    pub first_detections: usize,
    pub backfill_transactions: usize,
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub endpoints: Vec<EndpointSummary>,
    pub fastest_endpoint: Option<String>,
    pub has_data: bool,
    pub total_signatures: usize,
    pub backfill_signatures: usize,
}

pub fn compute_run_summary(comparator: &Comparator, endpoint_names: &[String]) -> RunSummary {
    let mut endpoint_stats: HashMap<String, EndpointStats> = HashMap::new();
    let expected_producers = endpoint_names.len();
    let mut total_signatures = 0usize;
    let mut backfill_signatures = 0usize;

    for endpoint_name in endpoint_names {
        endpoint_stats.insert(endpoint_name.clone(), EndpointStats::default());
    }

    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();
        if expected_producers > 0 && sig_data.len() != expected_producers {
            // Skip partial observations to mirror backend results
            continue;
        }

        let is_historical = sig_data
            .values()
            .any(|tx| tx.wallclock_secs < tx.start_wallclock_secs);

        if is_historical {
            backfill_signatures += 1;
            for endpoint in sig_data.keys() {
                if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                    stats.backfill_transactions += 1;
                }
            }
            continue;
        }

        let Some((first_endpoint, first_tx)) =
            sig_data.iter().min_by_key(|(_, tx)| tx.elapsed_since_start)
        else {
            continue;
        };

        total_signatures += 1;
        let first_endpoint_name = first_endpoint.clone();

        for (endpoint, tx) in sig_data.iter() {
            if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                stats.total_observations += 1;
                if endpoint == &first_endpoint_name {
                    stats.first_detections += 1;
                    stats.delays_ms.push(0.0);
                } else {
                    let delay_ms = diff_ms(tx, first_tx).max(0.0);
                    stats.delays_ms.push(delay_ms);
                }
            }
        }
    }

    let endpoints: Vec<EndpointSummary> = endpoint_stats
        .into_iter()
        .map(|(endpoint, stats)| build_summary(endpoint, stats, total_signatures))
        .collect();

    let has_data = total_signatures > 0;

    let fastest_endpoint = endpoints
        .iter()
        .filter(|summary| summary.valid_transactions > 0)
        .min_by(|a, b| compare_latency(a, b))
        .map(|summary| summary.name.clone());

    RunSummary {
        endpoints,
        fastest_endpoint,
        has_data,
        total_signatures,
        backfill_signatures,
    }
}

pub fn display_run_summary(summary: &RunSummary) {
    println!("\nFinished test results");
    println!("--------------------------------------------");

    if !summary.has_data {
        println!("Not enough data");
    } else {
        let fastest_name_ref = summary.fastest_endpoint.as_deref();
        let mut summary_rows: Vec<&EndpointSummary> = summary.endpoints.iter().collect();
        summary_rows.sort_by(|a, b| compare_latency(a, b));

        for summary in summary_rows {
            if summary.valid_transactions == 0 {
                println!("{}: Not enough data", summary.name);
                continue;
            }

            let raw_win_rate = format_percent(summary.first_share);
            let win_rate = if raw_win_rate == "—" {
                raw_win_rate
            } else {
                format!("{}%", raw_win_rate)
            };
            let is_fastest = fastest_name_ref == Some(summary.name.as_str());

            if is_fastest {
                println!(
                    "{}: Win rate {}, p50 0.00ms (fastest)",
                    summary.name, win_rate,
                );
            } else {
                let p50_delay = summary
                    .p50_delay_ms
                    .map(|v| format!("{:.2}ms", v))
                    .unwrap_or_else(|| "—".to_string());
                println!("{}: Win rate {}, p50 {}", summary.name, win_rate, p50_delay);
            }
        }
    }

    println!("\nDetailed test results");
    println!("--------------------------------------------");

    if !summary.has_data {
        println!("Not enough data");
        return;
    }

    let mut table_rows: Vec<&EndpointSummary> = summary.endpoints.iter().collect();
    table_rows.sort_by(|a, b| compare_latency(a, b));

    let mut table = Table::new();
    table.load_preset(table_preset());
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec![
        "Endpoint", "First %", "P50 ms", "P95 ms", "P99 ms", "Valid Tx", "Firsts", "Backfill",
    ]);

    for summary in table_rows {
        table.add_row(vec![
            summary.name.clone(),
            format_percent(summary.first_share),
            format_latency_value(summary.p50_delay_ms),
            format_latency_value(summary.p95_delay_ms),
            format_latency_value(summary.p99_delay_ms),
            summary.valid_transactions.to_string(),
            summary.first_detections.to_string(),
            summary.backfill_transactions.to_string(),
        ]);
    }

    println!("{}", table);
}

pub fn build_metrics_report(summary: &RunSummary) -> Value {
    let mut per_endpoint = Map::new();
    for endpoint in &summary.endpoints {
        let payload = json!({
            "first_detection_rate": endpoint.first_share,
            "p50_latency_ms": endpoint.p50_delay_ms,
            "p95_latency_ms": endpoint.p95_delay_ms,
            "p99_latency_ms": endpoint.p99_delay_ms,
            "observations": endpoint.valid_transactions,
            "first_detections": endpoint.first_detections,
            "backfill_transactions": endpoint.backfill_transactions,
        });
        per_endpoint.insert(endpoint.name.clone(), payload);
    }

    json!({
        "total_signatures": summary.total_signatures,
        "backfill_signatures": summary.backfill_signatures,
        "per_endpoint": per_endpoint
    })
}

fn diff_ms(tx: &TransactionData, first_tx: &TransactionData) -> f64 {
    let delta: Duration = tx
        .elapsed_since_start
        .saturating_sub(first_tx.elapsed_since_start);
    delta.as_secs_f64() * 1_000.0
}

// Keep the existing per-endpoint latency metrics unchanged. This separate table
// preserves the sign of processed_receive_time - deshred_receive_time.
pub fn display_deshred_deltas(
    comparator: &Comparator,
    endpoints: &[Endpoint],
    commitment: ArgsCommitment,
) {
    if !matches!(commitment, ArgsCommitment::Processed) {
        return;
    }
    let mut table = Table::new();
    table.load_preset(table_preset());
    table.set_header(vec![
        "Deshred",
        "Processed",
        "Matched Tx",
        "Δt P50 ms",
        "Δt P95 ms",
        "Δt P99 ms",
    ]);
    let mut has_pairs = false;
    for deshred in endpoints.iter().filter(|e| e.kind == EndpointKind::Deshred) {
        for processed in endpoints
            .iter()
            .filter(|e| e.kind == EndpointKind::Yellowstone)
        {
            has_pairs = true;
            let deltas = deshred_deltas(comparator, &deshred.name, &processed.name);
            table.add_row(vec![
                deshred.name.clone(),
                processed.name.clone(),
                deltas.len().to_string(),
                format_latency_value((!deltas.is_empty()).then(|| percentile(&deltas, 0.5))),
                format_latency_value((!deltas.is_empty()).then(|| percentile(&deltas, 0.95))),
                format_latency_value((!deltas.is_empty()).then(|| percentile(&deltas, 0.99))),
            ]);
        }
    }
    if has_pairs {
        println!(
            "\nΔt = processed receive time - deshred receive time (positive: deshred earlier)"
        );
        println!("{table}");
    }
}

fn deshred_deltas(comparator: &Comparator, deshred: &str, processed: &str) -> Vec<f64> {
    let mut deltas = Vec::new();
    for entry in comparator.iter() {
        let (Some(deshred), Some(processed)) =
            (entry.value().get(deshred), entry.value().get(processed))
        else {
            continue;
        };
        if deshred.wallclock_secs < deshred.start_wallclock_secs
            || processed.wallclock_secs < processed.start_wallclock_secs
        {
            continue;
        }
        let d = deshred.elapsed_since_start;
        let p = processed.elapsed_since_start;
        deltas.push(if p >= d {
            (p - d).as_secs_f64() * 1_000.0
        } else {
            -(d - p).as_secs_f64() * 1_000.0
        });
    }
    deltas.sort_by(f64::total_cmp);
    deltas
}

fn build_summary(
    endpoint: String,
    stats: EndpointStats,
    total_signatures: usize,
) -> EndpointSummary {
    let mut summary = EndpointSummary {
        name: endpoint,
        valid_transactions: stats.total_observations,
        first_detections: stats.first_detections,
        backfill_transactions: stats.backfill_transactions,
        ..Default::default()
    };

    if total_signatures > 0 {
        summary.first_share = stats.first_detections as f64 / total_signatures as f64;
    }

    if !stats.delays_ms.is_empty() {
        let mut sorted = stats.delays_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        summary.p50_delay_ms = Some(percentile(&sorted, 0.5));
        summary.p95_delay_ms = Some(percentile(&sorted, 0.95));
        summary.p99_delay_ms = Some(percentile(&sorted, 0.99));
    }

    summary
}

fn format_latency_value(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "—".to_string())
}

fn compare_latency(lhs: &EndpointSummary, rhs: &EndpointSummary) -> Ordering {
    match (lhs.p50_delay_ms, rhs.p50_delay_ms) {
        (Some(l), Some(r)) => l
            .partial_cmp(&r)
            .unwrap_or(Ordering::Equal)
            .then_with(|| lhs.name.cmp(&rhs.name)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => lhs.name.cmp(&rhs.name),
    }
}

fn format_percent(value: f64) -> String {
    if value.is_finite() {
        format!("{:.2}", value * 100.0)
    } else {
        "—".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deshred_delta_preserves_sign_and_excludes_unmatched_and_backfill() {
        let comparator = Comparator::new();
        let data = |millis| TransactionData {
            elapsed_since_start: Duration::from_millis(millis),
            wallclock_secs: 100.0,
            start_wallclock_secs: 99.0,
        };
        for (signature, d, p) in [("positive", 10, 25), ("negative", 30, 20), ("tie", 40, 40)] {
            comparator.record_observation("deshred", signature, data(d), 2);
            comparator.record_observation("processed", signature, data(p), 2);
        }
        comparator.record_observation("deshred", "unmatched", data(1), 2);
        comparator.record_observation("processed", "other-unmatched", data(100), 2);
        comparator.record_observation(
            "deshred",
            "historical",
            TransactionData {
                wallclock_secs: 98.0,
                ..data(1)
            },
            2,
        );
        comparator.record_observation("processed", "historical", data(10), 2);
        // Later repeated observations must not replace the earliest receive time.
        comparator.record_observation("deshred", "positive", data(100), 2);
        assert_eq!(
            deshred_deltas(&comparator, "deshred", "processed"),
            vec![-10.0, 0.0, 15.0]
        );
        let summary = compute_run_summary(&comparator, &["deshred".into(), "processed".into()]);
        assert_eq!(summary.total_signatures, 3);
        assert_eq!(summary.backfill_signatures, 1);
        assert!(summary.endpoints.iter().all(|e| e.valid_transactions == 3));
    }
}
