//! The dependencies of a Rust-code migration's crate, as its crate
//! capture has them.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// The source of a package from crates.io, as cargo writes it.
const CRATES_IO_SOURCE: &str = "registry+https://github.com/rust-lang/crates.io-index";

/// What the source of a package from a git repository starts with, as
/// cargo writes it.
const GIT_SOURCE_PREFIX: &str = "git+";

/// The crates of the migrations engine a Rust-code migration is built
/// with: the only path dependencies a migration can have.
const ENGINE_CRATES: [&str; 7] = [
    "waymark-blanket-impl-macros",
    "waymark-migrations-engine-migration",
    "waymark-migrations-rust-code",
    "waymark-migrations-rust-code-core",
    "waymark-migrations-rust-code-macros",
    "waymark-migrations-source-checksums-core",
    "waymark-migrations-source-checksums-proc-macro-helpers",
];

/// Why a package cannot be a captured dependency: nothing pins its
/// content.
#[derive(Debug, thiserror::Error)]
pub enum CapturedDependencyError {
    /// The package is a path dependency, and not a crate of the
    /// migrations engine.
    #[error("a path dependency outside the migrations engine")]
    PathDependency,

    /// The package is from a git repository, and its source does not say
    /// at which commit.
    #[error("a git dependency without its commit: {source_text}")]
    GitWithoutCommit {
        /// The package's source, as cargo writes it.
        source_text: String,
    },

    /// The package is from neither crates.io nor a git repository.
    #[error("a dependency from neither crates.io nor a git repository: {source_text}")]
    OtherSource {
        /// The package's source, as cargo writes it.
        source_text: String,
    },
}

/// A package a migration depends on, as its crate capture has it: the
/// package together with what pins its content.
///
/// It shows as its line among the capture's dependencies.
#[derive(Debug)]
enum CapturedDependency {
    /// A package from crates.io: a version there never changes content.
    /// Its line is `rmpv 1.3.1`.
    CratesIo {
        /// The package's name.
        name: String,

        /// The package's version.
        version: String,
    },

    /// A package from a git repository. Its line is
    /// `sqlx 0.9.0 (git+https://…#<commit>)`.
    Git {
        /// The package's name.
        name: String,

        /// The package's version.
        version: String,

        /// The package's source, as cargo writes it: it has the commit.
        source: String,
    },

    /// A crate of the migrations engine: its content is not pinned. Its
    /// line is `waymark-migrations-rust-code (path)`.
    EngineCrate {
        /// The crate's name.
        name: String,
    },
}

impl std::fmt::Display for CapturedDependency {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CratesIo { name, version } => write!(formatter, "{name} {version}"),
            Self::Git {
                name,
                version,
                source,
            } => write!(formatter, "{name} {version} ({source})"),
            Self::EngineCrate { name } => write!(formatter, "{name} (path)"),
        }
    }
}

impl CapturedDependency {
    /// The captured dependency of the package `name` at `version` from
    /// `source`, which is `None` for a path dependency.
    fn of(
        name: &str,
        version: &str,
        source: Option<&str>,
    ) -> Result<Self, CapturedDependencyError> {
        let Some(source) = source else {
            if ENGINE_CRATES.contains(&name) {
                return Ok(Self::EngineCrate {
                    name: name.to_owned(),
                });
            }

            return Err(CapturedDependencyError::PathDependency);
        };

        if source == CRATES_IO_SOURCE {
            return Ok(Self::CratesIo {
                name: name.to_owned(),
                version: version.to_owned(),
            });
        }

        if !source.starts_with(GIT_SOURCE_PREFIX) {
            return Err(CapturedDependencyError::OtherSource {
                source_text: source.to_owned(),
            });
        }

        let commit = source
            .rsplit_once('#')
            .map(|(_repository, commit)| commit)
            .unwrap_or_default();

        if commit.is_empty() {
            return Err(CapturedDependencyError::GitWithoutCommit {
                source_text: source.to_owned(),
            });
        }

        Ok(Self::Git {
            name: name.to_owned(),
            version: version.to_owned(),
            source: source.to_owned(),
        })
    }
}

/// A package is depended on that the dependency graph does not have.
#[derive(Debug, thiserror::Error)]
#[error("the dependency graph has no package {id}")]
pub struct UnknownPackageError {
    /// The id of the package.
    pub id: String,
}

/// The packages `root` depends on, directly or through others, going by
/// `dependencies`: each package's dependencies by its id. `root` is not
/// among them, unless it depends on itself through others.
fn transitive_dependencies<'a>(
    root: &'a str,
    dependencies: &BTreeMap<&'a str, Vec<&'a str>>,
) -> Result<BTreeSet<&'a str>, UnknownPackageError> {
    let mut reached = BTreeSet::new();
    let mut to_visit = vec![root];
    let mut visited = BTreeSet::new();

    while let Some(id) = to_visit.pop() {
        if !visited.insert(id) {
            continue;
        }

        let Some(dependencies_of_id) = dependencies.get(id) else {
            return Err(UnknownPackageError { id: id.to_owned() });
        };

        for dependency in dependencies_of_id {
            reached.insert(*dependency);
            to_visit.push(*dependency);
        }
    }

    Ok(reached)
}

/// A package that cannot be a captured dependency.
#[derive(Debug)]
pub struct RefusedPackage {
    /// The package's name.
    pub name: String,

    /// The package's version.
    pub version: String,

    /// Why it cannot be one.
    pub error: CapturedDependencyError,
}

/// The lines of `packages`, one package to a line.
fn refused_lines(packages: &[RefusedPackage]) -> String {
    let mut lines = String::new();

    for package in packages {
        lines.push_str(&format!(
            "\n    {} {}: {}",
            package.name, package.version, package.error
        ));
    }

    lines
}

/// Error returned by [`of_package`].
#[derive(Debug, thiserror::Error)]
pub enum OfPackageError {
    /// Cargo did not give the metadata of the package.
    #[error("running cargo for the dependencies of the package: {0}")]
    Metadata(#[source] cargo_metadata::Error),

    /// Cargo's metadata has no dependency graph.
    #[error("cargo's metadata has no dependency graph")]
    NoDependencyGraph,

    /// Cargo's dependency graph does not say which package it was asked
    /// for.
    #[error("cargo's dependency graph does not say which package it is of")]
    NoRoot,

    /// Cargo's dependency graph is not whole.
    #[error("cargo's metadata: {0}")]
    UnknownPackage(#[source] UnknownPackageError),

    /// The migration depends on packages that cannot be captured
    /// dependencies.
    #[error("the migration cannot depend on:{}", refused_lines(.0))]
    Refused(Vec<RefusedPackage>),
}

/// The packages the crate whose manifest is in `manifest_dir` depends on,
/// directly or through others, each as its line among a crate capture's
/// dependencies: the package together with what pins its content.
///
/// They are the crate's normal dependencies on every platform: neither
/// its development dependencies nor anyone's build dependencies. A
/// package from crates.io is `rmpv 1.3.1`, one from a git repository is
/// `sqlx 0.9.0 (git+https://…#<commit>)`, and a crate of the migrations
/// engine is `waymark-migrations-rust-code (path)`. Any other package is
/// refused: nothing pins its content.
///
/// Cargo is asked not to change the lock file. It fetches the packages of
/// the platforms other than this one when they are not there yet.
pub fn of_package(manifest_dir: &Path) -> Result<BTreeSet<String>, OfPackageError> {
    let metadata = cargo_metadata::MetadataCommand::new()
        .manifest_path(manifest_dir.join("Cargo.toml"))
        .other_options(vec!["--locked".to_owned()])
        .exec()
        .map_err(OfPackageError::Metadata)?;

    let Some(resolve) = &metadata.resolve else {
        return Err(OfPackageError::NoDependencyGraph);
    };

    let Some(root) = &resolve.root else {
        return Err(OfPackageError::NoRoot);
    };

    let mut normal_dependencies: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for node in &resolve.nodes {
        let normal_dependencies_of_node = node
            .deps
            .iter()
            .filter(|dependency| {
                dependency.dep_kinds.iter().any(|dependency_kind| {
                    matches!(dependency_kind.kind, cargo_metadata::DependencyKind::Normal)
                })
            })
            .map(|dependency| dependency.pkg.repr.as_str())
            .collect();

        normal_dependencies.insert(node.id.repr.as_str(), normal_dependencies_of_node);
    }

    let depended_on = transitive_dependencies(root.repr.as_str(), &normal_dependencies)
        .map_err(OfPackageError::UnknownPackage)?;

    let packages_by_id: BTreeMap<&str, &cargo_metadata::Package> = metadata
        .packages
        .iter()
        .map(|package| (package.id.repr.as_str(), package))
        .collect();

    let mut lines = BTreeSet::new();
    let mut refused = Vec::new();

    for id in depended_on {
        let Some(package) = packages_by_id.get(id) else {
            return Err(OfPackageError::UnknownPackage(UnknownPackageError {
                id: id.to_owned(),
            }));
        };

        let name = package.name.as_str();
        let version = package.version.to_string();
        let source = package.source.as_ref().map(|source| source.repr.as_str());

        match CapturedDependency::of(name, &version, source) {
            Ok(captured) => {
                lines.insert(captured.to_string());
            }
            Err(error) => refused.push(RefusedPackage {
                name: name.to_owned(),
                version,
                error,
            }),
        }
    }

    if !refused.is_empty() {
        return Err(OfPackageError::Refused(refused));
    }

    Ok(lines)
}

#[cfg(test)]
mod tests;
