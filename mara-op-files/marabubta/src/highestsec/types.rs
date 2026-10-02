// Marabunta - Licensed under the MIT License.
use serde::{Serialize, Deserialize};

/// ISO 3166-1 alpha-2 country code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CountryCode(pub [u8; 2]);

impl CountryCode {
    pub const FR: Self = Self(*b"FR");
    pub const DE: Self = Self(*b"DE");
    pub const ES: Self = Self(*b"ES");
    pub const IT: Self = Self(*b"IT");
    pub const US: Self = Self(*b"US");
    pub const GB: Self = Self(*b"GB");
    pub const NL: Self = Self(*b"NL");
    pub const BE: Self = Self(*b"BE");
    pub const PL: Self = Self(*b"PL");
    pub const RO: Self = Self(*b"RO");
    pub const SE: Self = Self(*b"SE");
    pub const PT: Self = Self(*b"PT");
    pub const GR: Self = Self(*b"GR");
    pub const CZ: Self = Self(*b"CZ");
    pub const HU: Self = Self(*b"HU");
    pub const AT: Self = Self(*b"AT");
    pub const BG: Self = Self(*b"BG");
    pub const DK: Self = Self(*b"DK");
    pub const FI: Self = Self(*b"FI");
    pub const SK: Self = Self(*b"SK");
    pub const IE: Self = Self(*b"IE");
    pub const HR: Self = Self(*b"HR");
    pub const LT: Self = Self(*b"LT");
    pub const SI: Self = Self(*b"SI");
    pub const LV: Self = Self(*b"LV");
    pub const EE: Self = Self(*b"EE");
    pub const CY: Self = Self(*b"CY");
    pub const LU: Self = Self(*b"LU");
    pub const MT: Self = Self(*b"MT");
    pub const CA: Self = Self(*b"CA");
    pub const AU: Self = Self(*b"AU");
    pub const NZ: Self = Self(*b"NZ");
    pub const TR: Self = Self(*b"TR");
    pub const NO: Self = Self(*b"NO");
    pub const IS: Self = Self(*b"IS");
    pub const ME: Self = Self(*b"ME");
    pub const MK: Self = Self(*b"MK");
    pub const AL: Self = Self(*b"AL");

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or("??")
    }
}

impl std::fmt::Display for CountryCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialOrd for CountryCode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CountryCode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_country_code_display() {
        assert_eq!(CountryCode::FR.to_string(), "FR");
        assert_eq!(CountryCode::DE.to_string(), "DE");
        assert_eq!(CountryCode::US.to_string(), "US");
    }

    #[test]
    fn test_country_code_as_str() {
        assert_eq!(CountryCode::FR.as_str(), "FR");
    }

    #[test]
    fn test_country_code_equality() {
        assert_eq!(CountryCode::FR, CountryCode::FR);
        assert_ne!(CountryCode::FR, CountryCode::DE);
    }

    #[test]
    fn test_country_code_serialization() {
        let cc = CountryCode::FR;
        let json = serde_json::to_string(&cc).unwrap();
        let cc2: CountryCode = serde_json::from_str(&json).unwrap();
        assert_eq!(cc, cc2);
    }
}
