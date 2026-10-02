// Marabunta - Licensed under the MIT License.
//! Compression benchmarks comparing different algorithms and compression levels
//!
//! Run with: cargo bench --bench compression_bench
//!
//! This benchmark suite compares:
//! - Compression ratios across algorithms (zstd, lz4, gzip)
//! - Compression and decompression speeds
//! - Performance at different compression levels
//! - Performance with different data types (text, binary, random)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use marabunta_compute::compression::{Algorithm, CompressionLevel, Compressor};
use std::time::Duration;

/// Generate test data that compresses well (repetitive text)
fn generate_compressible_text(size: usize) -> Vec<u8> {
    "Hello, this is highly compressible test data with lots of repetition! "
        .repeat(size / 70 + 1)
        .into_bytes()
        .into_iter()
        .take(size)
        .collect()
}

/// Generate binary data (somewhat compressible)
fn generate_binary_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// Generate random data (incompressible)
fn generate_random_data(size: usize) -> Vec<u8> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut data = Vec::with_capacity(size);
    let mut hasher = DefaultHasher::new();

    for i in 0..size {
        i.hash(&mut hasher);
        data.push((hasher.finish() % 256) as u8);
    }
    data
}

/// Benchmark compression across all algorithms
fn bench_compression(c: &mut Criterion) {
    let sizes = [1024, 64 * 1024, 1024 * 1024]; // 1KB, 64KB, 1MB
    let algorithms = [
        ("zstd", Algorithm::Zstd),
        ("lz4", Algorithm::Lz4),
        ("gzip", Algorithm::Gzip),
    ];

    let mut group = c.benchmark_group("compression");
    group.measurement_time(Duration::from_secs(5));

    for size in sizes {
        let data = generate_compressible_text(size);
        group.throughput(Throughput::Bytes(size as u64));

        for (name, algo) in &algorithms {
            let compressor = Compressor::new(*algo, CompressionLevel::Default);

            group.bench_with_input(
                BenchmarkId::new(format!("{}_compress", name), size),
                &data,
                |b, data| {
                    b.iter(|| compressor.compress(black_box(data)));
                },
            );
        }
    }

    group.finish();
}

/// Benchmark decompression across all algorithms
fn bench_decompression(c: &mut Criterion) {
    let sizes = [1024, 64 * 1024, 1024 * 1024];
    let algorithms = [
        ("zstd", Algorithm::Zstd),
        ("lz4", Algorithm::Lz4),
        ("gzip", Algorithm::Gzip),
    ];

    let mut group = c.benchmark_group("decompression");
    group.measurement_time(Duration::from_secs(5));

    for size in sizes {
        let data = generate_compressible_text(size);

        for (name, algo) in &algorithms {
            let compressor = Compressor::new(*algo, CompressionLevel::Default);
            let compressed = compressor.compress(&data).unwrap();

            // Report throughput based on decompressed size
            group.throughput(Throughput::Bytes(size as u64));

            group.bench_with_input(
                BenchmarkId::new(format!("{}_decompress", name), size),
                &compressed,
                |b, compressed| {
                    b.iter(|| compressor.decompress(black_box(compressed)));
                },
            );
        }
    }

    group.finish();
}

/// Benchmark compression levels for zstd
fn bench_zstd_levels(c: &mut Criterion) {
    let data = generate_compressible_text(256 * 1024); // 256KB
    let levels = [
        ("fast", CompressionLevel::Fast),
        ("default", CompressionLevel::Default),
        ("balanced", CompressionLevel::Balanced),
        ("max", CompressionLevel::Max),
    ];

    let mut group = c.benchmark_group("zstd_levels");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.measurement_time(Duration::from_secs(5));

    for (name, level) in &levels {
        let compressor = Compressor::new(Algorithm::Zstd, *level);

        group.bench_with_input(BenchmarkId::new("compress", *name), &data, |b, data| {
            b.iter(|| compressor.compress(black_box(data)));
        });
    }

    group.finish();
}

/// Benchmark compression levels for gzip
fn bench_gzip_levels(c: &mut Criterion) {
    let data = generate_compressible_text(256 * 1024);
    let levels = [
        ("fast", CompressionLevel::Fast),
        ("default", CompressionLevel::Default),
        ("max", CompressionLevel::Max),
    ];

    let mut group = c.benchmark_group("gzip_levels");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.measurement_time(Duration::from_secs(5));

    for (name, level) in &levels {
        let compressor = Compressor::new(Algorithm::Gzip, *level);

        group.bench_with_input(BenchmarkId::new("compress", *name), &data, |b, data| {
            b.iter(|| compressor.compress(black_box(data)));
        });
    }

    group.finish();
}

/// Benchmark with different data types
fn bench_data_types(c: &mut Criterion) {
    let size = 256 * 1024; // 256KB
    let data_types = [
        ("text", generate_compressible_text(size)),
        ("binary", generate_binary_data(size)),
        ("random", generate_random_data(size)),
    ];

    let mut group = c.benchmark_group("data_types");
    group.throughput(Throughput::Bytes(size as u64));
    group.measurement_time(Duration::from_secs(5));

    for (data_name, data) in &data_types {
        let compressor = Compressor::new(Algorithm::Zstd, CompressionLevel::Default);

        group.bench_with_input(
            BenchmarkId::new("zstd_compress", *data_name),
            data,
            |b, data| {
                b.iter(|| compressor.compress(black_box(data)));
            },
        );
    }

    group.finish();
}

/// Compare compression ratios (not a benchmark, but useful output)
fn report_compression_ratios(c: &mut Criterion) {
    let size = 1024 * 1024; // 1MB
    let data_types = [
        ("text", generate_compressible_text(size)),
        ("binary", generate_binary_data(size)),
        ("random", generate_random_data(size)),
    ];

    let algorithms = [
        ("zstd", Algorithm::Zstd),
        ("lz4", Algorithm::Lz4),
        ("gzip", Algorithm::Gzip),
    ];

    let levels = [
        ("fast", CompressionLevel::Fast),
        ("default", CompressionLevel::Default),
        ("max", CompressionLevel::Max),
    ];

    println!("\n=== Compression Ratio Report (1MB data) ===\n");
    println!(
        "{:<10} {:<10} {:<10} {:<15} {:<10}",
        "Algorithm", "Level", "Data Type", "Compressed", "Ratio"
    );
    println!("{}", "-".repeat(60));

    for (algo_name, algo) in &algorithms {
        for (level_name, level) in &levels {
            let compressor = Compressor::new(*algo, *level);

            for (data_name, data) in &data_types {
                let compressed = compressor.compress(data).unwrap();
                let ratio = compressed.len() as f64 / data.len() as f64;

                println!(
                    "{:<10} {:<10} {:<10} {:<15} {:.2}%",
                    algo_name,
                    level_name,
                    data_name,
                    format!("{} KB", compressed.len() / 1024),
                    ratio * 100.0
                );
            }
        }
    }

    // Also run a quick benchmark to keep criterion happy
    let mut group = c.benchmark_group("ratio_report");
    group.bench_function("noop", |b| b.iter(|| black_box(1 + 1)));
    group.finish();
}

/// Benchmark roundtrip (compress + decompress)
fn bench_roundtrip(c: &mut Criterion) {
    let data = generate_compressible_text(256 * 1024);
    let algorithms = [
        ("zstd", Algorithm::Zstd),
        ("lz4", Algorithm::Lz4),
        ("gzip", Algorithm::Gzip),
    ];

    let mut group = c.benchmark_group("roundtrip");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.measurement_time(Duration::from_secs(5));

    for (name, algo) in &algorithms {
        let compressor = Compressor::new(*algo, CompressionLevel::Default);

        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                let compressed = compressor.compress(black_box(data)).unwrap();
                compressor.decompress(black_box(&compressed)).unwrap()
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_compression,
    bench_decompression,
    bench_zstd_levels,
    bench_gzip_levels,
    bench_data_types,
    bench_roundtrip,
    report_compression_ratios,
);

criterion_main!(benches);
