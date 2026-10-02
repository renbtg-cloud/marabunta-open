<!-- Marabunta - Licensed under the MIT License.
# Marabunta Worker Privacy Policy

**Effective Date:** February 7, 2026
**Last Updated:** February 7, 2026

Marabunta Compute Project ("we", "us", or "our") operates the Marabunta Worker mobile application (the "App"). This Privacy Policy describes how we collect, use, and protect information when you use our App.

By installing and using the App, you agree to the collection and use of information as described in this policy.

---

## 1. What the App Does

Marabunta Worker is a voluntary distributed computing application. When you opt in, your device contributes its idle processing power to scientific and engineering workloads such as Monte Carlo simulations, parameter sweeps, and data processing tasks. The App only performs computation when your device is idle (screen off or locked) and, by default, only when plugged in and charging.

---

## 2. Information We Collect

### 2.1 Device Information (Collected Automatically)

When the App is active, it collects the following device information for the sole purpose of evaluating resource availability and routing compute tasks efficiently:

- **Hardware specifications:** CPU model, number of cores, total RAM, available storage
- **Operating system:** Android version, API level
- **Network type:** WiFi or cellular (to respect your data preferences)
- **Thermal state:** Device temperature status (to pause computation and protect your device)
- **Battery state:** Charging status and battery level (to respect battery policies)

### 2.2 Generated Identifiers

- **Node ID:** A randomly generated identifier (derived from device model and a timestamp hash) used to identify your device within the compute network. This identifier is not linked to your personal identity, Google account, or any advertising identifier.

### 2.3 Performance Metrics

- **Task statistics:** Number of tasks completed, computation time, success/failure rates. These metrics are stored locally on your device and used to optimize task routing.

---

## 3. Information We Do NOT Collect

We want to be explicit about what we do not access or collect:

- **Personal identity:** No name, email address, phone number, or account information
- **Contacts:** No access to your address book or contact list
- **Location:** No GPS, network-based location, or geolocation data (unless you explicitly opt in to geo-region preferences for task filtering, in which case only a coarse region label is stored -- never precise coordinates)
- **Photos, videos, or media:** No access to your camera, microphone, gallery, or media files
- **Messages:** No access to SMS, chat, or any messaging data
- **Browsing history:** No access to browser data or web activity
- **Files:** No access to your documents, downloads, or any user files outside the App's private directory
- **Advertising identifiers:** No collection of Google Advertising ID or similar tracking identifiers
- **Biometric data:** No fingerprint, face, or voice data
- **Financial information:** No payment or banking data

---

## 4. How Compute Tasks Work

When your device participates in distributed computing:

1. **Task receipt:** The App receives encrypted work units from the coordinator server. These work units contain computational instructions (scripts, parameters) -- never personal data.
2. **Execution:** Tasks are executed in a sandboxed environment within the App's private directory. Tasks cannot access your personal files, contacts, photos, or any other data on your device.
3. **Result delivery:** Computation results (numerical outputs, processed data) are sent back to the coordinator server over an encrypted TLS connection.
4. **Cleanup:** After task completion, all temporary task data (scripts, intermediate files, checkpoints) is deleted from your device. No task data persists beyond the active computation session.

---

## 5. Data Retention

- **Local statistics** (task count, uptime) are stored in Android SharedPreferences on your device. This data is deleted when you uninstall the App or clear App data.
- **Checkpoint data** for in-progress tasks is stored in the App's private directory and is automatically cleaned up after task completion or failure.
- **No persistent remote storage:** We do not maintain a database of your device information or activity history on our servers. Task results are delivered to the requesting party and are not associated with your device identity.

---

## 6. Data Sharing and Third Parties

- **We do not sell your data.** We do not sell, rent, trade, or otherwise transfer any information to third parties for marketing, advertising, or any commercial purpose.
- **No third-party analytics:** The App does not include any third-party analytics SDKs (such as Google Analytics, Firebase Analytics, Facebook SDK, or similar).
- **No advertising:** The App contains no advertisements and does not integrate with any advertising networks.
- **Swarm network:** Your device communicates with other nodes in the compute network solely for the purpose of task coordination and result delivery. Only computational data (task payloads and results) is exchanged -- never personal information.
- **Coordinator server:** The coordinator server receives task results and basic node metadata (Node ID, available resources) necessary for task routing. This data is not shared with any third party.

---

## 7. Data Security

We implement the following security measures to protect data in transit and at rest:

- **TLS encryption:** All communication between the App and the coordinator server uses TLS (Transport Layer Security) encryption.
- **Sandboxed execution:** Compute tasks run within the App's private directory and cannot access data outside the application sandbox.
- **No external storage:** The App does not write data to shared or external storage.
- **Working directory restrictions:** On Android, the App enforces strict working directory policies, denying access to system directories (`/system`, `/proc`, `/sys`, `/dev`, `/etc`) and only allowing operations within the App's private data directories.

---

## 8. Your Rights and Choices

### 8.1 Control Over Computation

You have full control over when and how your device participates:

- **Start and stop:** You can start or stop the compute service at any time via the App UI or the persistent notification.
- **Pause:** You can pause computation at any time without stopping the service.
- **Resource limits:** You can configure CPU usage limits, memory limits, and battery policies in the App settings.
- **Network preferences:** You can restrict computation to WiFi-only connections.

### 8.2 Data Deletion

- **Uninstall:** Uninstalling the App removes all locally stored data, including statistics, checkpoints, and the generated Node ID. No data remains on your device after uninstallation.
- **Clear App data:** You can clear the App's data through Android Settings at any time, which resets all local statistics and generates a new Node ID.

### 8.3 Opt Out

You can stop participating in distributed computing at any time by stopping the service or uninstalling the App. There is no lock-in period, no penalty, and no residual data collection after you stop.

---

## 9. GDPR Compliance (European Economic Area)

If you are located in the European Economic Area (EEA), the following applies:

- **Legal basis:** Our processing of device information is based on your explicit consent, given when you install the App and start the compute service. The App does not perform any processing until you explicitly initiate it.
- **Data minimization:** We collect only the minimum information necessary for task routing and device protection (thermal, battery, and resource monitoring).
- **Right of access:** You can view all collected data within the App's settings and statistics screens.
- **Right to erasure:** Uninstalling the App or clearing App data erases all collected information. Since we do not maintain persistent records on our servers tied to your device, there is no additional remote data to erase.
- **Right to restrict processing:** You can pause or stop the service at any time to halt all data processing.
- **Right to data portability:** The locally stored statistics are available on your device and can be accessed through standard Android backup mechanisms.
- **Data Protection Officer:** For GDPR-related inquiries, contact us at privacy@marabunta-compute.org.

---

## 10. Children's Privacy

The App is not directed at children under the age of 13 (or the applicable age of digital consent in your jurisdiction). We do not knowingly collect information from children under 13. If we become aware that a child under 13 has installed and used the App, we recommend that their parent or guardian uninstall the App, which will remove all locally stored data. The App does not collect personal information that would require parental consent under COPPA (Children's Online Privacy Protection Act) or similar regulations.

---

## 11. Changes to This Privacy Policy

We may update this Privacy Policy from time to time. Changes will be reflected in the "Last Updated" date at the top of this document. If we make material changes to how we handle data, we will notify users through the App or by updating this policy prominently. Continued use of the App after changes constitutes acceptance of the updated policy.

---

## 12. Contact Us

If you have questions or concerns about this Privacy Policy or the App's data practices, please contact us:

- **Email:** privacy@marabunta-compute.org
- **Project:** Marabunta Compute Project
- **Package name:** com.marabunta.worker

---

## 13. Summary

| Category | Details |
|----------|---------|
| Personal data collected | None |
| Device data collected | CPU, RAM, OS version, thermal/battery state (for resource management only) |
| Location data | Not collected (optional coarse region for geo-filtering only) |
| Data shared with third parties | None |
| Advertising | None |
| Analytics SDKs | None |
| Data retention | Local only; deleted on uninstall |
| User control | Full start/stop/pause/configure at any time |
| Encryption | TLS for all network communication |
| Children | Not directed at children under 13 |
