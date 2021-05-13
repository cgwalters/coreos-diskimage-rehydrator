use anyhow::{anyhow, Context, Result};
use coreos_stream_metadata::{fcos, rhcos};
use std::str::FromStr;
use strum_macros::{Display, EnumString};

/// The well-known distributions (operating systems) that generate
/// stream metadata.
#[derive(Debug, PartialEq, Eq, Clone, Copy, EnumString, Display)]
#[strum(serialize_all = "lowercase")]
// Not `pub` right now, just used in `stream_url_from_id()`
enum Distro {
    /// Fedora CoreOS
    FCOS,
    /// Red Hat Enterprise Linux CoreOS
    RHCOS,
}

/// Convert a string e.g. `fcos-stable` or `rhcos-4.8` to a stream URL.
/// The format is `<distro>-<stream>`.
pub(crate) fn stream_url_from_id(s: impl AsRef<str>) -> Result<String> {
    let s = s.as_ref();
    let mut it = s.splitn(2, '-');
    let distro = it.next().unwrap();
    let stream = it
        .next()
        .ok_or_else(|| anyhow!("Invalid stream ID, missing `-`: {}", s))?;
    let distro =
        Distro::from_str(distro).with_context(|| format!("Invalid distribution in {}", s))?;
    Ok(match distro {
        Distro::FCOS => fcos::StreamID::from_str(stream)
            .with_context(|| format!("Invalid stream: {}", stream))?
            .url(),
        Distro::RHCOS => rhcos::StreamID::from_str(stream)
            .with_context(|| format!("Invalid stream: {}", stream))?
            .url(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_url() {
        assert_eq!(
            stream_url_from_id("fcos-stable").unwrap(),
            fcos::StreamID::Stable.url()
        );
        assert_eq!(
            stream_url_from_id("rhcos-4.8").unwrap(),
            rhcos::StreamID::FourEight.url()
        );

        let invalid = &[
            "",
            "fcos",
            "moo",
            "rhcos",
            "fcos-",
            "-fcos",
            "fcos-blah",
            "fcos-blah-whee",
        ];
        for &elt in invalid {
            assert!(stream_url_from_id(elt).is_err());
        }
    }
}
