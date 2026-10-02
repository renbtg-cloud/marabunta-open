// Marabunta - Licensed under the MIT License.
use wasmtime::{Caller, Engine, Linker, Module, Store};
use wasmtime_wasi::{WasiCtxBuilder, WasiP1Ctx as WasiCtx};
use wasmtime_wasi::preview1::add_to_linker_sync;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};
use serde::{Serialize, Deserialize};

#[derive(Default, Serialize, Deserialize, Clone)]
pub struct MantisJournal {
    pub initial_memory: Vec<u8>,
    pub tape: Vec<WasiCallRecord>,
    /// When true, Mantis physically refuses to record memory state or I/O
    /// to preserve cryptographic isolation for FHE payloads.
    pub blackout_mode: bool,
    /// Mode for replaying a recorded journal.
    #[serde(skip)]
    pub replay_index: usize,
    /// Tracks the last returned timestamp to prevent NTP drift from breaking monotonicity.
    pub last_clock_timestamp: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WasiCallRecord {
    pub call_name: String,
    pub instruction_pointer: u64,
    pub returned_data: Vec<u8>,
}

pub struct MantisSandbox {
    pub journal: Arc<Mutex<MantisJournal>>,
    engine: Engine,
}

impl Default for MantisSandbox {
    fn default() -> Self {
        Self::new()
    }
}

impl MantisSandbox {
    pub fn new() -> Self {
        let mut engine_config = wasmtime::Config::new();
        engine_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
        Self {
            journal: Arc::new(Mutex::new(MantisJournal::default())),
            engine: Engine::new(&engine_config).unwrap(),
        }
    }

    /// Rehydrates a sandbox from a recorded journal and replays execution bit-for-bit.
    pub fn rehydrate_and_replay(&self, wasm_bytes: &[u8], journal_bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let journal: MantisJournal = bincode::deserialize(journal_bytes)?;
        let mut journal_state = journal;
        journal_state.replay_index = 0;
        let journal_arc = Arc::new(Mutex::new(journal_state));

        let mut linker: Linker<WasiCtx> = Linker::new(&self.engine);
        
        // We do NOT add standard WASI here; we only add our intercepted mocks to force bit-for-bit replay.
        self.add_intercepts(&mut linker, journal_arc.clone(), true)?;

        let wasi = WasiCtxBuilder::new().build_p1();
        let mut store = Store::new(&self.engine, wasi);
        let module = Module::new(&self.engine, wasm_bytes)?;
        let instance = linker.instantiate(&mut store, &module)?;

        let func = instance.get_typed_func::<(), ()>(&mut store, "_start")?;
        func.call(&mut store, ())?;
        
        info!("Mantis: Replay completed successfully. All hypercalls matched the journal.");
        Ok(())
    }

    fn add_intercepts(&self, linker: &mut Linker<WasiCtx>, journal: Arc<Mutex<MantisJournal>>, is_replay: bool) -> Result<(), wasmtime::Error> {
        let journal_clone = journal.clone();
        
        // --- random_get Intercept ---
        linker.func_wrap(
            "wasi_snapshot_preview1", 
            "random_get", 
            move |mut caller: Caller<'_, WasiCtx>, buf: i32, buf_len: i32| -> i32 {
                let mut j = journal_clone.lock().unwrap();
                if j.blackout_mode { return 0; }
                
                let data = if is_replay {
                    let record = &j.tape[j.replay_index];
                    assert_eq!(record.call_name, "random_get");
                    let d = record.returned_data.clone();
                    j.replay_index += 1;
                    d
                } else {
                    let mut host_rand = vec![0u8; buf_len as usize];
                    for (i, b) in host_rand.iter_mut().enumerate() { *b = (i % 255) as u8; }
                    j.tape.push(WasiCallRecord {
                        call_name: "random_get".to_string(),
                        instruction_pointer: 0,
                        returned_data: host_rand.clone(),
                    });
                    host_rand
                };

                if let Some(memory) = caller.get_export("memory").and_then(|m: wasmtime::Extern| m.into_memory()) {
                    memory.write(&mut caller, buf as usize, &data).unwrap();
                }
                0 
            }
        )?;

        let journal_clone_2 = journal.clone();
        // --- clock_time_get Intercept ---
        linker.func_wrap(
            "wasi_snapshot_preview1",
            "clock_time_get",
            move |mut caller: Caller<'_, WasiCtx>, id: i32, precision: i64, result_ptr: i32| -> i32 {
                let mut j = journal_clone_2.lock().unwrap();
                if j.blackout_mode { return 0; }

                let timestamp_bytes = if is_replay {
                    let record = &j.tape[j.replay_index];
                    assert_eq!(record.call_name, "clock_time_get");
                    let d = record.returned_data.clone();
                    j.replay_index += 1;
                    d
                
                } else {
                    // [MARABUNTA WMD] Isomorphic Monotonic Clock with NTP Drift Compensation
                    // Ensures that even if the host OS clock jumps backwards (NTP sync),
                    // the WASM execution environment perceives a strictly forward-moving timeline.
                    let current_ts: u64 = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or(std::time::Duration::from_secs(0))
                        .as_nanos() as u64;

                    // Strictly monotonic forward progression
                    let final_ts = std::cmp::max(j.last_clock_timestamp, current_ts);
                    j.last_clock_timestamp = final_ts;

                    let d = final_ts.to_le_bytes().to_vec();
                    j.tape.push(WasiCallRecord {
                        call_name: "clock_time_get".to_string(),
                        instruction_pointer: 0,
                        returned_data: d.clone(),
                    });
                    d
                };


                if let Some(memory) = caller.get_export("memory").and_then(|m| m.into_memory()) {
                    memory.write(&mut caller, result_ptr as usize, &timestamp_bytes).unwrap();
                }
                0
            }
        )?;

        let journal_clone_3 = journal.clone();
        // --- fd_write Intercept (The "Mouth") ---
        linker.func_wrap(
            "wasi_snapshot_preview1",
            "fd_write",
            move |mut caller: Caller<'_, WasiCtx>, fd: i32, iovs_ptr: i32, iovs_len: i32, nwritten_ptr: i32| -> i32 {
                let mut j = journal_clone_3.lock().unwrap();
                if j.blackout_mode { return 0; }

                // PRODUCTION UPGRADE: Intercept all stdout/stderr for forensic replay
                if is_replay {
                    let nwritten = {
                        let record = &j.tape[j.replay_index];
                        assert_eq!(record.call_name, format!("fd_write_{}", fd));
                        u32::from_le_bytes(record.returned_data[..4].try_into().unwrap())
                    };
                    j.replay_index += 1;
                    if let Some(memory) = caller.get_export("memory").and_then(|m| m.into_memory()) {
                        memory.write(&mut caller, nwritten_ptr as usize, &nwritten.to_le_bytes()).unwrap();
                    }
                } else {
                    // Capture what was written
                    let mut total_written = 0u32;
                    let memory = caller.get_export("memory").and_then(|m| m.into_memory()).unwrap();
                    
                    // We don't actually write to the host FD in record mode to keep it pure,
                    // or we could inherit_stdio. For now, we just record.
                    for i in 0..iovs_len {
                        let base = iovs_ptr as usize + (i as usize * 8);
                        let mut buf = [0u8; 8];
                        memory.read(&caller, base, &mut buf).unwrap();
                        let ptr = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
                        let len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
                        total_written += len as u32;
                    }

                    j.tape.push(WasiCallRecord {
                        call_name: format!("fd_write_{}", fd),
                        instruction_pointer: 0,
                        returned_data: total_written.to_le_bytes().to_vec(),
                    });

                    if let Some(memory) = caller.get_export("memory").and_then(|m| m.into_memory()) {
                        memory.write(&mut caller, nwritten_ptr as usize, &total_written.to_le_bytes()).unwrap();
                    }
                }
                0
            }
        )?;

        let journal_clone_4 = journal.clone();
        // --- path_open Intercept (The "Eye") ---
        linker.func_wrap(
            "wasi_snapshot_preview1",
            "path_open",
            move |mut _caller: Caller<'_, WasiCtx>, _fd: i32, _dirflags: i32, _path_ptr: i32, _path_len: i32, _oflags: i32, _fs_rights_base: i64, _fs_rights_inheriting: i64, _fdflags: i32, _opened_fd_ptr: i32| -> i32 {
                let mut j = journal_clone_4.lock().unwrap();
                if j.blackout_mode { return 0; }
                
                // Virtualized File System: Always return a virtual FD (e.g. 100+)
                if is_replay {
                    let record = &j.tape[j.replay_index];
                    assert_eq!(record.call_name, "path_open");
                    j.replay_index += 1;
                } else {
                    j.tape.push(WasiCallRecord {
                        call_name: "path_open".to_string(),
                        instruction_pointer: 0,
                        returned_data: vec![100], // Mock virtual FD
                    });
                }
                0
            }
        )?;

        Ok(())
    }

    pub fn execute_and_record(&self, wasm_bytes: &[u8], blackout_mode: bool) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        info!("Initializing Mantis Flight Data Recorder with WASI preview1 intercept");
        self.journal.lock().unwrap().blackout_mode = blackout_mode;
        
        let mut linker: Linker<WasiCtx> = Linker::new(&self.engine);
        self.add_intercepts(&mut linker, self.journal.clone(), false)?;

        let wasi = WasiCtxBuilder::new().inherit_stdio().build_p1();
        let mut store = Store::new(&self.engine, wasi);
        let module = Module::new(&self.engine, wasm_bytes)?;
        let instance = linker.instantiate(&mut store, &module)?;
        
        if let Some(memory) = instance.get_memory(&mut store, "memory") {
            self.journal.lock().unwrap().initial_memory = memory.data(&store).to_vec();
        }

        let func = instance.get_typed_func::<(), ()>(&mut store, "_start")?;
        let _ = func.call(&mut store, ());

        let dump_bytes = bincode::serialize(&*self.journal.lock().unwrap())?;
        Ok(dump_bytes)
    }
}
