import re

with open("src/swarm/mod.rs", "r") as f:
    code = f.read()

target = r"""            // Wire Management Dashboard UI path
            if let Ok\(cwd\) = std::env::current_dir\(\) \{
                let ui_path = cwd\.join\("management-ui"\)\.to_string_lossy\(\)\.to_string\(\);
                api_server\.state\.management_ui_dir = Some\(ui_path\);
            \}"""

replacement = r"""            // Wire Management Dashboard UI path
            if let Ok(cwd) = std::env::current_dir() {
                let ui_path = cwd.join("management-ui").to_string_lossy().to_string();
                api_server.state.management_ui_dir = Some(ui_path);
            }
            
            // GUI VACUUM FIX
            api_server.state.event_bus = Some(Arc::clone(&self.event_bus));
            api_server.state.vision_store = Some(Arc::clone(&self.vision_store));
            api_server.state.fleet_manager = Some(Arc::clone(&self.fleet_manager));
            api_server.state.healthcheck_engine = Some(Arc::clone(&self.healthcheck_engine));
            api_server.state.sovereignty_manager = Some(Arc::clone(&self.sovereignty_manager));
            api_server.state.membrane_engine = Some(Arc::clone(&self.membrane_engine));
            api_server.state.crossing_log = Some(Arc::clone(&self.crossing_log));
            api_server.state.agreement_store = Some(Arc::clone(&self.agreement_store));
            api_server.state.lending_meter = Some(Arc::clone(&self.lending_meter));
            api_server.state.constellation_builder = Some(Arc::clone(&self.constellation_builder));
            api_server.state.alert_engine = Some(Arc::clone(&self.alert_engine));
            api_server.state.sla_monitor = Some(Arc::clone(&self.sla_monitor));
            api_server.state.capacity_planner = Some(Arc::clone(&self.capacity_planner));
            api_server.state.audit_log = Some(Arc::clone(&self.audit_log));
            api_server.state.swarm_metrics = Some(Arc::clone(&self.swarm_metrics));
            api_server.state.verification_engine = Some(Arc::clone(&self.verification_engine));
            api_server.state.chunk_planner = Some(Arc::clone(&self.chunk_planner));
            api_server.state.webhook_engine = Some(Arc::clone(&self.webhook_engine));
            api_server.state.pricing_engine = Some(Arc::clone(&self.pricing_engine));
            api_server.state.streaming_engine = Some(Arc::clone(&self.streaming_engine));
            api_server.state.job_scheduler = Some(Arc::clone(&self.job_scheduler));
            api_server.state.wasm_executor = Some(Arc::clone(&self.wasm_executor));
            api_server.state.combo_registry = Some(Arc::clone(&self.combo_registry));"""

code = re.sub(target, replacement, code)

with open("src/swarm/mod.rs", "w") as f:
    f.write(code)

