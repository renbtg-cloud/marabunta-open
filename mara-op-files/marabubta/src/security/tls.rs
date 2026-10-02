// Marabunta - Licensed under the MIT License.
//! TLS configuration and context management using rustls.
//!
//! This module provides TLS configuration for both server and client contexts,
//! supporting production deployments with full certificate validation as well
//! as development scenarios with self-signed certificates.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::errors::{SecurityError, SecurityResult};

/// TLS operation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    /// TLS disabled (plain TCP) - not recommended for production
    #[default]
    Disabled,
    /// TLS enabled with full certificate validation
    Enabled,
    /// TLS enabled but accepts self-signed certificates (for development)
    DevMode,
}

impl TlsMode {
    /// Check if TLS is enabled in any form
    pub fn is_enabled(&self) -> bool {
        !matches!(self, TlsMode::Disabled)
    }

    /// Check if we're in development mode
    pub fn is_dev_mode(&self) -> bool {
        matches!(self, TlsMode::DevMode)
    }
}

/// TLS configuration for the Marabunta Compute system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// TLS operation mode
    pub mode: TlsMode,
    /// Path to the certificate file (PEM format)
    pub cert_path: Option<PathBuf>,
    /// Path to the private key file (PEM format)
    pub key_path: Option<PathBuf>,
    /// Path to the CA certificate file for verifying peers (PEM format)
    pub ca_path: Option<PathBuf>,
    /// Require client certificates (mutual TLS)
    #[serde(default)]
    pub require_client_cert: bool,
    /// Verify hostname in certificates
    #[serde(default = "default_verify_hostname")]
    pub verify_hostname: bool,
    /// ALPN protocols to advertise
    #[serde(default)]
    pub alpn_protocols: Vec<String>,
}

fn default_verify_hostname() -> bool {
    true
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            mode: TlsMode::Disabled,
            cert_path: None,
            key_path: None,
            ca_path: None,
            require_client_cert: false,
            verify_hostname: true,
            alpn_protocols: Vec::new(),
        }
    }
}

impl TlsConfig {
    /// Create a new TLS config builder
    pub fn builder() -> TlsConfigBuilder {
        TlsConfigBuilder::default()
    }

    /// Create a disabled TLS config
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Create a development mode TLS config with self-signed certificates
    pub fn dev_mode(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            mode: TlsMode::DevMode,
            cert_path: Some(cert_path.into()),
            key_path: Some(key_path.into()),
            ca_path: None,
            require_client_cert: false,
            verify_hostname: false,
            alpn_protocols: Vec::new(),
        }
    }

    /// Check if TLS is enabled
    pub fn is_enabled(&self) -> bool {
        self.mode.is_enabled()
    }

    /// Validate the configuration
    pub fn validate(&self) -> SecurityResult<()> {
        if !self.mode.is_enabled() {
            return Ok(());
        }

        // Certificate and key are required when TLS is enabled
        if self.cert_path.is_none() {
            return Err(SecurityError::TlsConfig(
                "Certificate path required when TLS is enabled".to_string(),
            ));
        }

        if self.key_path.is_none() {
            return Err(SecurityError::TlsConfig(
                "Private key path required when TLS is enabled".to_string(),
            ));
        }

        // Check if files exist
        if let Some(ref path) = self.cert_path {
            if !path.exists() {
                return Err(SecurityError::Certificate(format!(
                    "Certificate file not found: {}",
                    path.display()
                )));
            }
        }

        if let Some(ref path) = self.key_path {
            if !path.exists() {
                return Err(SecurityError::PrivateKey(format!(
                    "Private key file not found: {}",
                    path.display()
                )));
            }
        }

        if let Some(ref path) = self.ca_path {
            if !path.exists() {
                return Err(SecurityError::Certificate(format!(
                    "CA certificate file not found: {}",
                    path.display()
                )));
            }
        }

        Ok(())
    }

    /// Load certificates from the configured path
    pub fn load_certificates(&self) -> SecurityResult<Vec<CertificateDer<'static>>> {
        let cert_path = self.cert_path.as_ref().ok_or_else(|| {
            SecurityError::TlsConfig("Certificate path not configured".to_string())
        })?;

        load_certificates(cert_path)
    }

    /// Load the private key from the configured path
    pub fn load_private_key(&self) -> SecurityResult<PrivateKeyDer<'static>> {
        let key_path = self.key_path.as_ref().ok_or_else(|| {
            SecurityError::TlsConfig("Private key path not configured".to_string())
        })?;

        load_private_key(key_path)
    }

    /// Load CA certificates from the configured path
    pub fn load_ca_certificates(&self) -> SecurityResult<Vec<CertificateDer<'static>>> {
        let ca_path = self
            .ca_path
            .as_ref()
            .ok_or_else(|| SecurityError::TlsConfig("CA path not configured".to_string()))?;

        load_certificates(ca_path)
    }

    /// Build a server TLS configuration
    ///
    /// Returns a rustls ServerConfig suitable for use with axum/tokio servers.
    pub fn server_config(&self) -> SecurityResult<Arc<ServerTlsConfig>> {
        self.validate()?;

        let certs = self.load_certificates()?;
        let key = self.load_private_key()?;

        // Configure client certificate verification
        let mut config = if self.require_client_cert {
            let ca_certs = self.load_ca_certificates()?;
            let mut root_store = RootCertStore::empty();
            for cert in ca_certs {
                root_store.add(cert).map_err(|e| {
                    SecurityError::Certificate(format!("Failed to add CA certificate: {}", e))
                })?;
            }

            let verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
                .build()
                .map_err(|e| {
                    SecurityError::TlsConfig(format!("Failed to build client verifier: {}", e))
                })?;

            ServerTlsConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    SecurityError::TlsConfig(format!("Failed to set protocol versions: {}", e))
                })?
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| {
                    SecurityError::TlsConfig(format!("Failed to build server config: {}", e))
                })?
        } else {
            ServerTlsConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    SecurityError::TlsConfig(format!("Failed to set protocol versions: {}", e))
                })?
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| {
                    SecurityError::TlsConfig(format!("Failed to build server config: {}", e))
                })?
        };

        // Set ALPN protocols if configured
        if !self.alpn_protocols.is_empty() {
            config.alpn_protocols = self
                .alpn_protocols
                .iter()
                .map(|p| p.as_bytes().to_vec())
                .collect();
        }

        Ok(Arc::new(config))
    }

    /// Build a client TLS configuration
    ///
    /// Returns a rustls ClientConfig suitable for connecting to TLS servers.
    pub fn client_config(&self) -> SecurityResult<Arc<ClientTlsConfig>> {
        self.validate()?;

        let mut root_store = RootCertStore::empty();

        // Add CA certificates if configured
        if let Some(ref ca_path) = self.ca_path {
            let ca_certs = load_certificates(ca_path)?;
            for cert in ca_certs {
                root_store.add(cert).map_err(|e| {
                    SecurityError::Certificate(format!("Failed to add CA certificate: {}", e))
                })?;
            }
        }

        let provider = Arc::new(rustls::crypto::ring::default_provider());

        let config_builder = if self.mode.is_dev_mode() && self.ca_path.is_none() {
            // In dev mode without CA, use a dangerous verifier that accepts any cert
            ClientTlsConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    SecurityError::TlsConfig(format!("Failed to set protocol versions: {}", e))
                })?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(DangerousAcceptAnyCert))
        } else {
            ClientTlsConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    SecurityError::TlsConfig(format!("Failed to set protocol versions: {}", e))
                })?
                .with_root_certificates(root_store)
        };

        // Add client certificate if we have one (for mutual TLS)
        let config = if self.cert_path.is_some() && self.key_path.is_some() {
            let certs = self.load_certificates()?;
            let key = self.load_private_key()?;
            config_builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| {
                    SecurityError::TlsConfig(format!("Failed to set client certificate: {}", e))
                })?
        } else {
            config_builder.with_no_client_auth()
        };

        Ok(Arc::new(config))
    }

    /// Build a client TLS configuration for mutual TLS (mTLS)
    ///
    /// This requires both client certificates and CA certificates.
    pub fn mtls_client_config(&self) -> SecurityResult<Arc<ClientTlsConfig>> {
        if self.cert_path.is_none() || self.key_path.is_none() {
            return Err(SecurityError::TlsConfig(
                "Client certificate and key required for mutual TLS".to_string(),
            ));
        }

        if self.ca_path.is_none() && !self.mode.is_dev_mode() {
            return Err(SecurityError::TlsConfig(
                "CA certificate required for mutual TLS (or use dev mode)".to_string(),
            ));
        }

        self.client_config()
    }
}

/// Builder for TlsConfig
#[derive(Debug, Default)]
pub struct TlsConfigBuilder {
    mode: TlsMode,
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
    ca_path: Option<PathBuf>,
    require_client_cert: bool,
    verify_hostname: bool,
    alpn_protocols: Vec<String>,
}

impl TlsConfigBuilder {
    /// Set the TLS mode
    pub fn mode(mut self, mode: TlsMode) -> Self {
        self.mode = mode;
        self
    }

    /// Enable TLS
    pub fn enabled(mut self) -> Self {
        self.mode = TlsMode::Enabled;
        self
    }

    /// Enable development mode (accepts self-signed certificates)
    pub fn dev_mode(mut self) -> Self {
        self.mode = TlsMode::DevMode;
        self.verify_hostname = false;
        self
    }

    /// Set the certificate path
    pub fn cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.cert_path = Some(path.into());
        self
    }

    /// Set the private key path
    pub fn key_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.key_path = Some(path.into());
        self
    }

    /// Set the CA certificate path
    pub fn ca_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_path = Some(path.into());
        self
    }

    /// Require client certificates (enable mutual TLS)
    pub fn require_client_cert(mut self, require: bool) -> Self {
        self.require_client_cert = require;
        self
    }

    /// Enable mutual TLS (shortcut for require_client_cert(true))
    pub fn mtls(self) -> Self {
        self.require_client_cert(true)
    }

    /// Set hostname verification
    pub fn verify_hostname(mut self, verify: bool) -> Self {
        self.verify_hostname = verify;
        self
    }

    /// Add an ALPN protocol
    pub fn alpn_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.alpn_protocols.push(protocol.into());
        self
    }

    /// Set ALPN protocols
    pub fn alpn_protocols(mut self, protocols: Vec<String>) -> Self {
        self.alpn_protocols = protocols;
        self
    }

    /// Build the TlsConfig
    pub fn build(self) -> SecurityResult<TlsConfig> {
        let config = TlsConfig {
            mode: self.mode,
            cert_path: self.cert_path,
            key_path: self.key_path,
            ca_path: self.ca_path,
            require_client_cert: self.require_client_cert,
            verify_hostname: self.verify_hostname,
            alpn_protocols: self.alpn_protocols,
        };

        config.validate()?;
        Ok(config)
    }
}

// Re-export rustls types for convenience
pub use rustls::pki_types::{CertificateDer, PrivateKeyDer};
pub use rustls::server::WebPkiClientVerifier;
pub use rustls::{ClientConfig as ClientTlsConfig, RootCertStore, ServerConfig as ServerTlsConfig};

/// Load PEM certificates from a file
pub fn load_certificates(path: impl AsRef<Path>) -> SecurityResult<Vec<CertificateDer<'static>>> {
    let file = File::open(path.as_ref()).map_err(|e| {
        SecurityError::Certificate(format!(
            "Failed to open certificate file {}: {}",
            path.as_ref().display(),
            e
        ))
    })?;

    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SecurityError::Certificate(format!("Failed to parse certificates: {}", e)))?;

    if certs.is_empty() {
        return Err(SecurityError::Certificate(
            "No certificates found in file".to_string(),
        ));
    }

    Ok(certs)
}

/// Load a PEM private key from a file
pub fn load_private_key(path: impl AsRef<Path>) -> SecurityResult<PrivateKeyDer<'static>> {
    let file = File::open(path.as_ref()).map_err(|e| {
        SecurityError::PrivateKey(format!(
            "Failed to open private key file {}: {}",
            path.as_ref().display(),
            e
        ))
    })?;

    let mut reader = BufReader::new(file);

    // Try to read as PKCS#8 first, then RSA, then EC
    loop {
        match rustls_pemfile::read_one(&mut reader) {
            Ok(Some(rustls_pemfile::Item::Pkcs8Key(key))) => {
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            Ok(Some(rustls_pemfile::Item::Pkcs1Key(key))) => {
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            Ok(Some(rustls_pemfile::Item::Sec1Key(key))) => {
                return Ok(PrivateKeyDer::Sec1(key));
            }
            Ok(Some(_)) => continue, // Skip other items
            Ok(None) => break,
            Err(e) => {
                return Err(SecurityError::PrivateKey(format!(
                    "Failed to parse private key: {}",
                    e
                )))
            }
        }
    }

    Err(SecurityError::PrivateKey(
        "No private key found in file".to_string(),
    ))
}

/// Dangerous certificate verifier that accepts any certificate.
/// Only for development mode.
#[derive(Debug)]
struct DangerousAcceptAnyCert;

impl rustls::client::danger::ServerCertVerifier for DangerousAcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // WARNING: This is insecure and should only be used in development
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        // Support all signature schemes
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    // Test certificate and key (self-signed ECDSA P-256, for testing only)
    const TEST_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIBdzCCAR2gAwIBAgIUEYLeX+bnLaqdkOVOxBY/TBdYTm4wCgYIKoZIzj0EAwIw
ETEPMA0GA1UEAwwGdGVzdGNhMB4XDTI2MDIwNTEyMDMwNVoXDTI3MDIwNTEyMDMw
NVowETEPMA0GA1UEAwwGdGVzdGNhMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE
os3Y/FYpIY0S9TY2vQlgYV8s023LceFMctPT0Fi5c4Wjz+VI1xEcyOHQZA4c9eD0
opYeNtJ6vSPgTQTRtK0+FKNTMFEwHQYDVR0OBBYEFDiz6FuH1VzSfHsji8y4N3B/
GqqFMB8GA1UdIwQYMBaAFDiz6FuH1VzSfHsji8y4N3B/GqqFMA8GA1UdEwEB/wQF
MAMBAf8wCgYIKoZIzj0EAwIDSAAwRQIhAMj/UN12yNsxdB5dJ/Wt+b6Y5BkaYWAO
KITam/UxyyQwAiBy7qY2VpPqjlli1Jdmq1zQjNXZUEiO4Skd2z4MCHTaxg==
-----END CERTIFICATE-----"#;

    const TEST_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgdR9oHlm/3AlCAhCu
nC1lWEaFpXj+ts0oNgHU54d6gxyhRANCAASizdj8VikhjRL1Nja9CWBhXyzTbctx
4Uxy09PQWLlzhaPP5UjXERzI4dBkDhz14PSilh420nq9I+BNBNG0rT4U
-----END PRIVATE KEY-----"#;

    fn create_test_files(dir: &TempDir) -> (PathBuf, PathBuf) {
        let cert_path = dir.path().join("test.crt");
        let key_path = dir.path().join("test.key");

        let mut cert_file = File::create(&cert_path).unwrap();
        cert_file.write_all(TEST_CERT_PEM.as_bytes()).unwrap();

        let mut key_file = File::create(&key_path).unwrap();
        key_file.write_all(TEST_KEY_PEM.as_bytes()).unwrap();

        (cert_path, key_path)
    }

    #[test]
    fn test_tls_mode_is_enabled() {
        assert!(!TlsMode::Disabled.is_enabled());
        assert!(TlsMode::Enabled.is_enabled());
        assert!(TlsMode::DevMode.is_enabled());
    }

    #[test]
    fn test_tls_mode_is_dev_mode() {
        assert!(!TlsMode::Disabled.is_dev_mode());
        assert!(!TlsMode::Enabled.is_dev_mode());
        assert!(TlsMode::DevMode.is_dev_mode());
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(!config.is_enabled());
        assert_eq!(config.mode, TlsMode::Disabled);
        assert!(config.cert_path.is_none());
        assert!(config.key_path.is_none());
        assert!(config.ca_path.is_none());
    }

    #[test]
    fn test_tls_config_builder() {
        let dir = TempDir::new().unwrap();
        let (cert_path, key_path) = create_test_files(&dir);

        let config = TlsConfig::builder()
            .dev_mode()
            .cert_path(&cert_path)
            .key_path(&key_path)
            .require_client_cert(false)
            .build()
            .unwrap();

        assert!(config.is_enabled());
        assert_eq!(config.mode, TlsMode::DevMode);
        assert_eq!(config.cert_path, Some(cert_path));
        assert_eq!(config.key_path, Some(key_path));
        assert!(!config.require_client_cert);
        assert!(!config.verify_hostname);
    }

    #[test]
    fn test_tls_config_validation_missing_cert() {
        let result = TlsConfig::builder()
            .enabled()
            .key_path("/nonexistent/key.pem")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SecurityError::TlsConfig(_)));
    }

    #[test]
    fn test_tls_config_validation_missing_key() {
        let dir = TempDir::new().unwrap();
        let (cert_path, _) = create_test_files(&dir);

        let result = TlsConfig::builder().enabled().cert_path(cert_path).build();

        assert!(result.is_err());
    }

    #[test]
    fn test_tls_config_validation_file_not_found() {
        let result = TlsConfig::builder()
            .enabled()
            .cert_path("/nonexistent/cert.pem")
            .key_path("/nonexistent/key.pem")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_load_certificates() {
        let dir = TempDir::new().unwrap();
        let (cert_path, _) = create_test_files(&dir);

        let certs = load_certificates(&cert_path).unwrap();
        assert!(!certs.is_empty());
    }

    #[test]
    fn test_load_private_key() {
        let dir = TempDir::new().unwrap();
        let (_, key_path) = create_test_files(&dir);

        let key = load_private_key(&key_path).unwrap();
        assert!(matches!(key, PrivateKeyDer::Pkcs8(_)));
    }

    #[test]
    fn test_tls_config_dev_mode_shortcut() {
        let dir = TempDir::new().unwrap();
        let (cert_path, key_path) = create_test_files(&dir);

        let config = TlsConfig::dev_mode(&cert_path, &key_path);

        assert!(config.is_enabled());
        assert!(config.mode.is_dev_mode());
        assert!(!config.verify_hostname);
    }

    #[test]
    fn test_tls_config_alpn_protocols() {
        let dir = TempDir::new().unwrap();
        let (cert_path, key_path) = create_test_files(&dir);

        let config = TlsConfig::builder()
            .dev_mode()
            .cert_path(&cert_path)
            .key_path(&key_path)
            .alpn_protocol("h2")
            .alpn_protocol("http/1.1")
            .build()
            .unwrap();

        assert_eq!(config.alpn_protocols, vec!["h2", "http/1.1"]);
    }

    #[test]
    fn test_server_config_creation() {
        let dir = TempDir::new().unwrap();
        let (cert_path, key_path) = create_test_files(&dir);

        let config = TlsConfig::builder()
            .dev_mode()
            .cert_path(&cert_path)
            .key_path(&key_path)
            .build()
            .unwrap();

        let server_config = config.server_config();
        assert!(server_config.is_ok());
    }

    #[test]
    fn test_client_config_dev_mode() {
        let dir = TempDir::new().unwrap();
        let (cert_path, key_path) = create_test_files(&dir);

        let config = TlsConfig::builder()
            .dev_mode()
            .cert_path(&cert_path)
            .key_path(&key_path)
            .build()
            .unwrap();

        let client_config = config.client_config();
        assert!(client_config.is_ok());
    }

    #[test]
    fn test_mtls_client_config_requires_certs() {
        let config = TlsConfig::builder().enabled().build();

        // Should fail because no certs configured
        assert!(config.is_err());
    }
}
