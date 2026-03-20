slotmap::new_key_type! {
    pub struct Id;
}

impl core::fmt::Display for Id {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0.as_ffi())
    }
}

impl core::str::FromStr for Id {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ffi = s.parse()?;
        let key_data = slotmap::KeyData::from_ffi(ffi);
        Ok(Self::from(key_data))
    }
}
