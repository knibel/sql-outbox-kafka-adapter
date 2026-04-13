# Copilot Instructions

## Semantic Versioning

This project uses [Semantic Versioning 2.0.0](https://semver.org/) (`MAJOR.MINOR.PATCH`).
The authoritative version lives in **`pom.xml`** (`<version>`). The Docker image is tagged
with the same version, so every meaningful change must include a version bump in `pom.xml`.

### When to bump

| Change type | Version segment | Example |
|---|---|---|
| Breaking change to public API or configuration | **MAJOR** | `1.0.0` → `2.0.0` |
| New backwards-compatible feature or capability | **MINOR** | `1.0.0` → `1.1.0` |
| Backwards-compatible bug-fix, dependency update, or refactor | **PATCH** | `1.0.0` → `1.0.1` |

### SNAPSHOT convention

- During development the version in `pom.xml` carries the `-SNAPSHOT` suffix, e.g. `1.2.3-SNAPSHOT`.
- When cutting a release, remove the suffix so the version becomes `1.2.3`.

### Checklist for every pull request

1. **Has the version in `pom.xml` been increased?**  
   - If the PR introduces any user-visible change (feature, fix, breaking change) the version **must** be bumped.
   - Pure documentation or CI-only changes may omit a version bump.
2. **Is the bump type correct?**  
   - Breaking API / config change → MAJOR.
   - New feature, backwards-compatible → MINOR.
   - Bug-fix, dependency update, internal refactor → PATCH.
3. **Is the `-SNAPSHOT` suffix present for development work?**  
   - Branch / PR work should keep `-SNAPSHOT`; remove it only when releasing.

### How the Docker image version is derived

The Docker publish workflow (`docker-publish.yml`) reads the version directly from `pom.xml`
using `mvn help:evaluate`. The resulting image is tagged as both `<version>` (e.g. `1.2.3`)
and `latest`. There is no separate versioning file to maintain.
