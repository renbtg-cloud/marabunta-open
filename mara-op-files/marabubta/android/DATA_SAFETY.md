<!-- Marabunta - Licensed under the MIT License.
# Data Safety Declaration (Google Play Store)

## Data Collected
- **Device identifier**: Generated node ID (device model + timestamp hash). Not linked to user identity.
- **Performance data**: CPU usage, memory usage, task completion rates. Used for task routing optimization.

## Data Shared
- **Computation results**: Task outputs are sent to the Marabunta coordinator server. Contains only computational results, no personal data.
- **Network metadata**: Public IP address (via STUN) for peer-to-peer connectivity.

## Data NOT Collected
- Contacts, location, photos, files, messages, browsing history, or any personal data.
- The app does not access any user content on the device.

## Security
- All coordinator communication uses TLS encryption.
- Task payloads are encrypted in transit.
- No data stored on external storage.

## Data Retention
- Statistics (task count, tokens earned) stored locally in SharedPreferences.
- Checkpoint data stored in app-private directory, cleaned up after task completion.
- No data persisted on remote servers beyond task result delivery.
