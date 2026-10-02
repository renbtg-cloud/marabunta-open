# Marabunta - Licensed under the MIT License.
import torch
import torch.nn as nn
from marabunta_torch import DistributedDiLoCo

# 1. The data scientist defines their standard PyTorch architecture
class DeepSeekMini(nn.Module):
    def __init__(self):
        super().__init__()
        self.dense1 = nn.Linear(1024, 4096)
        self.relu = nn.ReLU()
        self.dense2 = nn.Linear(4096, 1024)

    def forward(self, x):
        x = self.dense1(x)
        x = self.relu(x)
        return self.dense2(x)

model = DeepSeekMini()

# 2. They swap torch.optim.AdamW for the Marabunta Trojan Horse
# They do not need to compile WASM, write Rust, or know what a DHT is.
optimizer = DistributedDiLoCo(
    params=model.parameters(),
    model=model,
    dataset_uri="s3://corporate-datalake/training-shard-01",
    lr=0.001,
    sync_interval=500,        # The DiLoCo 'H' mathematical pause
    outer_momentum=0.7,       # Nesterov global momentum
    strategy="adaptive",      # Autonomously failover to Kademlia if TCP drops
    api_url="http://localhost:8080"
)

# 3. They execute their standard training loop
print("Starting training loop...")
dummy_data = torch.randn(32, 1024)

# The moment they call `step()`, Marabunta intercepts the graph, compiles it,
# and scatters it to 10,000 nodes worldwide.
output = model(dummy_data)
loss = output.sum()
loss.backward()

optimizer.step() 
print("Local python script finished. Swarm is orchestrating the epochs.")