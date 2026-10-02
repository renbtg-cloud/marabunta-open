<!-- Marabunta - Licensed under the MIT License.
# Marabunta Physical Deployment

## The Bootstrap Layer
To run a physical demonstration across 10,000 nodes, the nodes must know how to discover each other over the public internet and negotiate NAT firewalls.

You must stand up a centralized Bootstrap and STUN/TURN server before deploying the Swarm.

### Step 1: Provision a Public VM
Spin up an AWS EC2 instance or a DigitalOcean Droplet with a static public IP address.
Install `docker` and `docker-compose`.

### Step 2: Configure and Boot
SSH into the public VM and clone this repository.
Navigate to the `deployments/bootstrap` directory.

Create a `.env` file with your instance's public IP:
```bash
PUBLIC_IP=203.0.113.55
TURN_SECRET=super_secret_string_123
```

Start the infrastructure:
```bash
docker-compose up -d --build
```

This will launch:
1.  **marabunta-bootstrap**: The native `marabunta-swarm` binary acting as the Kademlia anchor.
2.  **coturn**: A production STUN/TURN server allowing edge nodes (like Starlink) to punch through strict Carrier-Grade NATs via WebRTC/ICE.

### Step 3: Deploy the Fleet
Now you can hand the SpaceX engineer the `marabunta-swarm` executable.
When they start the binary on their laptops or satellites, they must point it at your public bootstrap node:

```bash
./marabunta-swarm --start --bootstrap "203.0.113.55:4200" --turn-server "turn:203.0.113.55:3478?transport=udp" --turn-secret "super_secret_string_123"
```