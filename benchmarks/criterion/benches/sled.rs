#![allow(dead_code)]

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Instant;

use jemallocator::Jemalloc;

use sled::Config;

#[cfg_attr(
    // only enable jemalloc on linux and macos by default
    any(target_os = "linux", target_os = "macos"),
    global_allocator
)]
static ALLOC: Jemalloc = Jemalloc;

const BATCH_SIZES: &[usize] =
    &[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];

fn counter() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    static C: AtomicUsize = AtomicUsize::new(0);

    C.fetch_add(1, Relaxed)
}

/// Generates a random number in `0..n`.
fn random(n: u32) -> u32 {
    use std::cell::Cell;
    use std::num::Wrapping;

    thread_local! {
        static RNG: Cell<Wrapping<u32>> = Cell::new(Wrapping(1406868647));
    }

    RNG.with(|rng| {
        // This is the 32-bit variant of Xorshift.
        //
        // Source: https://en.wikipedia.org/wiki/Xorshift
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        rng.set(x);

        // This is a fast alternative to `x % n`.
        //
        // Author: Daniel Lemire
        // Source: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        ((x.0 as u64).wrapping_mul(n as u64) >> 32) as u32
    })
}

fn sled_bulk_load(c: &mut Criterion) {
    let mut count = 0_u32;
    let mut bytes = |len| -> Vec<u8> {
        count += 1;
        count.to_be_bytes().iter().cycle().take(len).copied().collect()
    };

    let mut bench = |key_len, val_len| {
        let db = Config::new()
            .path(format!("bulk_k{}_v{}", key_len, val_len))
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        c.bench_function(
            &format!("bulk load key/value lengths {}/{}", key_len, val_len),
            |b| {
                b.iter(|| {
                    db.insert(bytes(key_len), bytes(val_len)).unwrap();
                })
            },
        );
    };

    for key_len in &[10_usize, 128, 256, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048, 4096, 8192] {
            bench(*key_len, *val_len)
        }
    }
}

fn mk_persy() -> persy::Persy {
    use persy::*;
    let temp = tempfile::tempfile().unwrap();
    Persy::create_from_file(temp.try_clone().unwrap()).unwrap();
    Persy::open_from_file(temp, Config::new()).unwrap()
}

fn sled_monotonic_crud(c: &mut Criterion) {
    let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

    c.bench_function("monotonic inserts", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            db.insert(count.to_be_bytes(), vec![]).unwrap();
        })
    });

    c.bench_function("monotonic gets", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            db.get(count.to_be_bytes()).unwrap();
        })
    });

    c.bench_function("monotonic removals", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            db.remove(count.to_be_bytes()).unwrap();
        })
    });
}

fn sled_random_crud(c: &mut Criterion) {
    const SIZE: u32 = 65536;

    let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

    c.bench_function("random inserts", |b| {
        b.iter(|| {
            let k = random(SIZE).to_be_bytes();
            db.insert(k, vec![]).unwrap();
        })
    });

    c.bench_function("random gets", |b| {
        b.iter(|| {
            let k = random(SIZE).to_be_bytes();
            db.get(k).unwrap();
        })
    });

    c.bench_function("random removals", |b| {
        b.iter(|| {
            let k = random(SIZE).to_be_bytes();
            db.remove(k).unwrap();
        })
    });
}

fn sled_empty_opens(c: &mut Criterion) {
    let _ = std::fs::remove_dir_all("empty_opens");
    c.bench_function("empty opens", |b| {
        b.iter(|| {
            Config::new()
                .path(format!("empty_opens/{}.db", counter()))
                .flush_every_ms(None)
                .open()
                .unwrap()
        })
    });
    let _ = std::fs::remove_dir_all("empty_opens");
}

fn tx_sled_bulk_load(c: &mut Criterion) {
    let mut bench = |key_len, val_len| {
        let db = Config::new()
            .path(format!("bulk_k{}_v{}", key_len, val_len))
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        c.bench_function(
            &format!("bulk load key/value lengths {}/{}", key_len, val_len),
            |b| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    db.transaction::<_, _, ()>(|db| {
                        let mut count = 0_u32;
                        let mut bytes = |len| -> Vec<u8> {
                            count += 1;
                            count
                                .to_be_bytes()
                                .iter()
                                .cycle()
                                .take(len)
                                .copied()
                                .collect()
                        };

                        for _ in 0..iters {
                            db.insert(bytes(key_len), bytes(val_len))?;
                        }
                        Ok(())
                    })
                    .unwrap();
                    start.elapsed()
                });
            },
        );
    };

    for key_len in &[10_usize, 128, 256, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048, 4096, 8192] {
            bench(*key_len, *val_len)
        }
    }
}

fn tx_sled_monotonic_crud(c: &mut Criterion) {
    let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

    let mut bench = |batch_size: usize| {
        c.bench_function(
            &format!("monotonic inserts tx, batch size: {}", batch_size),
            |b| {
                b.iter_custom(|iters| {
                    let all_iters: Vec<_> = (0..iters).collect();
                    let start = Instant::now();
                    for chunk in all_iters.chunks(batch_size) {
                        db.transaction::<_, _, ()>(|db| {
                            for count in chunk {
                                db.insert(&count.to_be_bytes(), vec![])?;
                            }
                            Ok(())
                        })
                        .unwrap();
                    }
                    start.elapsed()
                })
            },
        );

        c.bench_function(
            &format!("monotonic gets tx, batch size: {}", batch_size),
            |b| {
                b.iter_custom(|iters| {
                    let all_iters: Vec<_> = (0..iters).collect();
                    let start = Instant::now();
                    for chunk in all_iters.chunks(batch_size) {
                        db.transaction::<_, _, ()>(|db| {
                            for count in chunk {
                                db.get(&count.to_be_bytes())?;
                            }
                            Ok(())
                        })
                        .unwrap();
                    }
                    start.elapsed()
                })
            },
        );

        c.bench_function(
            &format!("monotonic removals tx, batch size: {}", batch_size),
            |b| {
                b.iter_custom(|iters| {
                    let all_iters: Vec<_> = (0..iters).collect();
                    let start = Instant::now();
                    for chunk in all_iters.chunks(batch_size) {
                        db.transaction::<_, _, ()>(|db| {
                            for count in chunk {
                                db.remove(&count.to_be_bytes())?;
                            }
                            Ok(())
                        })
                        .unwrap();
                    }
                    start.elapsed()
                })
            },
        );
    };

    for bs in BATCH_SIZES {
        bench(*bs);
    }
}

fn tx_sled_random_crud(c: &mut Criterion) {
    const SIZE: u32 = 65536;

    let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

    let mut bench = |batch_size: usize| {
        c.bench_function(
            &format!("random inserts tx, batch size: {}", batch_size),
            |b| {
                b.iter_custom(|iters| {
                    let all_iters: Vec<_> = (0..iters).collect();
                    let start = Instant::now();
                    for chunk in all_iters.chunks(batch_size) {
                        db.transaction::<_, _, ()>(|db| {
                            for _ in chunk {
                                let k = random(SIZE).to_be_bytes();
                                db.insert(&k, vec![])?;
                            }
                            Ok(())
                        })
                        .unwrap();
                    }
                    start.elapsed()
                })
            },
        );

        c.bench_function(
            &format!("random gets tx, batch size: {}", batch_size),
            |b| {
                b.iter_custom(|iters| {
                    let all_iters: Vec<_> = (0..iters).collect();
                    let start = Instant::now();
                    for chunk in all_iters.chunks(batch_size) {
                        db.transaction::<_, _, ()>(|db| {
                            for _ in chunk {
                                let k = random(SIZE).to_be_bytes();
                                db.get(&k)?;
                            }
                            Ok(())
                        })
                        .unwrap();
                    }
                    start.elapsed()
                })
            },
        );

        c.bench_function(
            &format!("random removals tx, batch size: {}", batch_size),
            |b| {
                b.iter_custom(|iters| {
                    let all_iters: Vec<_> = (0..iters).collect();
                    let start = Instant::now();
                    for chunk in all_iters.chunks(batch_size) {
                        db.transaction::<_, _, ()>(|db| {
                            for _ in chunk {
                                let k = random(SIZE).to_be_bytes();
                                db.remove(&k)?;
                            }
                            Ok(())
                        })
                        .unwrap();
                    }
                    start.elapsed()
                })
            },
        );
    };

    for bs in BATCH_SIZES {
        bench(*bs);
    }
}

fn persy_bulk_load(c: &mut Criterion) {
    use persy::*;

    let mut count = 0_u32;
    let mut bytes = |len| -> ByteVec {
        count += 1;
        ByteVec(count.to_be_bytes().iter().cycle().take(len).copied().collect())
    };

    let mut bench = |key_len, val_len| {
        c.bench_function(
            &format!(
                "persy: bulk load key/value lengths {}/{}",
                key_len, val_len
            ),
            |b| {
                let db = mk_persy();
                let mut tx = db.begin().unwrap();
                tx.create_index::<ByteVec, ByteVec>("", ValueMode::EXCLUSIVE)
                    .unwrap();

                b.iter(|| {
                    tx.put("", bytes(key_len), bytes(val_len)).unwrap();
                });
                tx.prepare_commit().unwrap().commit().unwrap();
            },
        );
    };

    for key_len in &[10_usize, 128, 256, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048, 4096, 8192] {
            bench(*key_len, *val_len)
        }
    }
}

fn persy_monotonic_crud(c: &mut Criterion) {
    use persy::*;

    let db = mk_persy();
    let mut tx = db.begin().unwrap();
    tx.create_index::<ByteVec, ByteVec>("", ValueMode::EXCLUSIVE).unwrap();
    tx.prepare_commit().unwrap().commit().unwrap();

    c.bench_function("persy: monotonic inserts", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            let mut tx = db.begin().unwrap();
            tx.put::<ByteVec, ByteVec>(
                "",
                count.to_be_bytes().to_vec().into(),
                vec![].into(),
            )
            .unwrap();
            tx.prepare_commit().unwrap().commit().unwrap();
        })
    });

    c.bench_function("persy: monotonic gets", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            let mut tx = db.begin().unwrap();
            tx.get::<ByteVec, ByteVec>(
                "",
                &count.to_be_bytes().to_vec().into(),
            )
            .unwrap();
            tx.prepare_commit().unwrap().commit().unwrap();
        })
    });

    c.bench_function("persy: monotonic removals", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            let mut tx = db.begin().unwrap();
            tx.remove::<ByteVec, ByteVec>(
                "",
                count.to_be_bytes().to_vec().into(),
                None,
            )
            .unwrap();
            tx.prepare_commit().unwrap().commit().unwrap();
        })
    });
}

fn persy_random_crud(c: &mut Criterion) {
    const SIZE: u32 = 65536;

    use persy::*;

    let db = mk_persy();
    let mut tx = db.begin().unwrap();
    tx.create_index::<ByteVec, ByteVec>("", ValueMode::EXCLUSIVE).unwrap();
    tx.prepare_commit().unwrap().commit().unwrap();

    c.bench_function("persy: random inserts", |b| {
        b.iter(|| {
            let k = random(SIZE).to_be_bytes().to_vec().into();
            let mut tx = db.begin().unwrap();
            tx.put::<ByteVec, ByteVec>("", k, vec![].into()).unwrap();
            tx.prepare_commit().unwrap().commit().unwrap();
        })
    });

    c.bench_function("persy: random gets", |b| {
        b.iter(|| {
            let k = random(SIZE).to_be_bytes().to_vec();
            let mut tx = db.begin().unwrap();
            tx.get::<ByteVec, ByteVec>("", &k.into()).unwrap();
            tx.prepare_commit().unwrap().commit().unwrap();
        })
    });

    c.bench_function("persy: random removals", |b| {
        b.iter(|| {
            let k = random(SIZE).to_be_bytes().to_vec().into();
            let mut tx = db.begin().unwrap();
            tx.remove::<ByteVec, ByteVec>("", k, None).unwrap();
            tx.prepare_commit().unwrap().commit().unwrap();
        })
    });
}

fn persy_empty_opens(c: &mut Criterion) {
    c.bench_function("persy: empty opens", |b| b.iter(|| mk_persy()));
}

criterion_group!(
    benches,
    //
    // sled_bulk_load,
    // tx_sled_bulk_load,
    // persy_bulk_load,
    //
    sled_monotonic_crud,
    tx_sled_monotonic_crud,
    // persy_monotonic_crud,
    //
    sled_random_crud,
    tx_sled_random_crud,
    // persy_random_crud,
    //
    // sled_empty_opens,
    // persy_empty_opens,
);
criterion_main!(benches);
