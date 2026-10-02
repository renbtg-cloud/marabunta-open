<!-- Marabunta - Licensed under the MIT License.
<a id="chapter-4-the-leaderless-dag"></a>
## Chapter 4: The Leaderless DAG

Traditional leader-election algorithms (Raft, Paxos) instantly fail in a high-churn environment of 100 million edge devices. Marabunta utilizes a True >2/3 BFT Strongly-Seen DAG (Hashgraph).

*Academic Semiotic Format:*
$$ S(x, y) = | \{ n \in Nodes \mid \exists z \in E : \text{creator}(z) = n \land x \rightarrow z \land z \rightarrow y \} | > \frac{2}{3} N $$

*Applied Python Vectorization:*
```python
import numpy as np

def is_strongly_seen(x_idx: int, y_idx: int, dag_adj: np.ndarray, creators: np.ndarray, total: int) -> bool:
    '''
    Computes S(x,y) via vector multiplication.
    dag_adj[i][j] == 1 if path exists from i to j.
    '''
    # Find all intermediate events 'z' that x sees and that see y
    valid_paths = dag_adj[x_idx, :] & dag_adj[:, y_idx]
    
    # Extract unique creators of those intermediate 'z' events
    bridging_nodes = np.unique(creators[valid_paths == 1])
    
    # Assert > 2/3 Byzantine Threshold
    return len(bridging_nodes) > (2/3 * total)
```
