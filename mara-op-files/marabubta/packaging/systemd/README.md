<!-- Marabunta - Licensed under the MIT License.
# Marabunta Compute Systemd Services

This directory contains systemd service files for running Marabunta Compute as system services.

## Installation

### Quick Install

```bash
# Run the install script (as root)
sudo ./install-services.sh
```

### Manual Install

1. Copy service files:
```bash
sudo cp marabunta-*.service /etc/systemd/system/
```

2. Create the marabunta user:
```bash
sudo useradd -r -s /bin/false -d /var/lib/marabunta marabunta
```

3. Create directories:
```bash
sudo mkdir -p /var/lib/marabunta /var/log/marabunta /etc/marabunta
sudo chown -R marabunta:marabunta /var/lib/marabunta /var/log/marabunta
```

4. Copy configuration:
```bash
sudo cp coordinator.env.example /etc/marabunta/coordinator.env
sudo cp master.env.example /etc/marabunta/master.env
sudo cp worker.env.example /etc/marabunta/worker.env
```

5. Reload systemd:
```bash
sudo systemctl daemon-reload
```

## Usage

### Start Services

```bash
# Start coordinator
sudo systemctl start marabunta-coordinator

# Start master
sudo systemctl start marabunta-master

# Start worker
sudo systemctl start marabunta-worker
```

### Enable on Boot

```bash
sudo systemctl enable marabunta-coordinator
sudo systemctl enable marabunta-master
sudo systemctl enable marabunta-worker
```

### Check Status

```bash
sudo systemctl status marabunta-coordinator
sudo systemctl status marabunta-master
sudo systemctl status marabunta-worker
```

### View Logs

```bash
# Follow coordinator logs
journalctl -u marabunta-coordinator -f

# View master logs from last hour
journalctl -u marabunta-master --since "1 hour ago"

# View all marabunta logs
journalctl -u "marabunta-*" -f
```

## Multiple Workers

Use the template service to run multiple workers on one machine:

```bash
# Enable workers 1-4
sudo systemctl enable marabunta-worker@1
sudo systemctl enable marabunta-worker@2
sudo systemctl enable marabunta-worker@3
sudo systemctl enable marabunta-worker@4

# Start all enabled workers
sudo systemctl start marabunta-worker@{1..4}
```

Each worker instance uses its own config file (`/etc/marabunta/worker-N.toml`) and data directory (`/var/lib/marabunta/worker-N`).

## Configuration

### Environment Files

- `/etc/marabunta/coordinator.env` - Coordinator environment
- `/etc/marabunta/master.env` - Master environment
- `/etc/marabunta/worker.env` - Worker environment

### Config Files

Create TOML config files for more detailed configuration:
- `/etc/marabunta/coordinator.toml`
- `/etc/marabunta/master.toml`
- `/etc/marabunta/worker.toml`

## Troubleshooting

### Service won't start

1. Check logs: `journalctl -u marabunta-coordinator -n 50`
2. Verify permissions: `ls -la /var/lib/marabunta`
3. Test manually: `sudo -u marabunta /usr/local/bin/marabunta-coordinator --config /etc/marabunta/coordinator.toml`

### Permission denied

```bash
sudo chown -R marabunta:marabunta /var/lib/marabunta /var/log/marabunta
sudo chmod 750 /var/lib/marabunta /var/log/marabunta
```

### Port already in use

Check what's using the port:
```bash
sudo ss -tlnp | grep 8080
```
