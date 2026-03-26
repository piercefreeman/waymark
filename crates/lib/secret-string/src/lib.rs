use zeroize::Zeroizing;

#[repr(transparent)]
pub struct SecretStr(str);

impl SecretStr {
    pub const fn new(secret: &str) -> &Self {
        // SAFETY: SecretStr is repr(transparent) over str.
        unsafe { &*(secret as *const str as *const SecretStr) }
    }

    pub const fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretStr([REDACTED])")
    }
}

impl std::fmt::Display for SecretStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// String secret that is zeroized on drop and supports parsing from env/config.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    pub fn new(secret: String) -> Self {
        Self(Zeroizing::new(secret))
    }

    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_secret_str(&self) -> &SecretStr {
        SecretStr::new(self.expose_secret())
    }

    pub fn into_inner(self) -> String {
        self.0.to_string()
    }
}

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        Self::new(value.to_owned())
    }
}

impl From<&SecretStr> for SecretString {
    fn from(value: &SecretStr) -> Self {
        Self::from(value.expose_secret())
    }
}

impl AsRef<SecretStr> for SecretString {
    fn as_ref(&self) -> &SecretStr {
        self.as_secret_str()
    }
}

impl std::borrow::Borrow<SecretStr> for SecretString {
    fn borrow(&self) -> &SecretStr {
        self.as_secret_str()
    }
}

impl std::ops::Deref for SecretString {
    type Target = SecretStr;

    fn deref(&self) -> &Self::Target {
        self.as_secret_str()
    }
}

impl std::str::FromStr for SecretString {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString([REDACTED])")
    }
}

impl std::fmt::Display for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl serde::Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("[REDACTED]")
    }
}

impl<'de> serde::Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(Self::from(raw))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;

    use super::{SecretStr, SecretString};

    #[test]
    fn from_str_keeps_value() {
        let secret = SecretString::from_str("postgresql://example").expect("infallible parser");
        assert_eq!(secret.expose_secret(), "postgresql://example");
    }

    #[test]
    fn debug_and_display_are_redacted() {
        let secret = SecretString::from("postgresql://example");

        assert_eq!(format!("{secret:?}"), "SecretString([REDACTED])");
        assert_eq!(format!("{secret}"), "[REDACTED]");
    }

    #[test]
    fn serialize_is_redacted() {
        let secret = SecretString::from("postgresql://example");
        let val = serde_json::to_value(&secret).expect("serialize secret");

        assert_eq!(val, json!("[REDACTED]"));
    }

    #[test]
    fn deserialize_parses_string() {
        let secret: SecretString =
            serde_json::from_str("\"postgresql://example\"").expect("deserialize secret");

        assert_eq!(secret.expose_secret(), "postgresql://example");
    }

    #[test]
    fn secret_str_borrowed_view_and_to_owned() {
        let secret = SecretString::from("postgresql://example");
        let borrowed: &SecretStr = secret.as_secret_str();

        assert_eq!(borrowed.expose_secret(), "postgresql://example");
        let roundtrip = SecretString::from(borrowed);
        assert_eq!(roundtrip.expose_secret(), "postgresql://example");
    }

    #[test]
    fn secret_string_deref_to_secret_str() {
        let secret = SecretString::from("postgresql://example");
        let borrowed: &SecretStr = &secret;

        assert_eq!(borrowed.expose_secret(), "postgresql://example");
    }
}
