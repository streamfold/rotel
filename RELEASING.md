# Releasing

This document describes the automated release process for Rotel.

## Overview

Rotel uses an automated release workflow powered by GitHub Actions. The process is designed to be simple and consistent, requiring minimal manual intervention.

## Release Workflow

The release process consists of three main GitHub Actions workflows:

1. **Bump Version** (`bump-version.yml`) - Manually triggered to create a version bump PR
2. **Auto Tag** (`auto-tag.yml`) - Automatically creates a git tag when a version bump PR is merged
3. **Auto Release** (`auto-release.yml`) - Automatically creates a GitHub release when a tag is pushed

### Step-by-Step Process

#### 1. Trigger a Version Bump

To start a release, manually trigger the **Bump Version** workflow from the GitHub Actions tab:

1. Go to **Actions** → **Bump Version**
2. Click **Run workflow**
3. Select the bump type:
   - `patch` - for bug fixes (e.g., 0.1.0 → 0.1.1)
   - `minor` - for new features (e.g., 0.1.0 → 0.2.0)
   - `major` - for breaking changes (e.g., 0.1.0 → 1.0.0)
4. Click **Run workflow**

This will:
- Read the current version from `Cargo.toml`
- Calculate the new version based on your selection
- Create a new branch named `re/bump-for-<version>`
- Update `Cargo.toml` with the new version
- Run `cargo check` to update `Cargo.lock`
- Create a pull request with these changes

#### 2. Review and Merge the Version Bump PR

The workflow will create a PR titled "Release: Bump version to X.Y.Z". Review the PR to ensure:

- The version number is correct
- `Cargo.toml` and `Cargo.lock` are properly updated
- Any other necessary changes are included

When ready, merge the PR.

**Optional: Skip Automatic Tagging**

If you want to prevent automatic tagging after merging (e.g., you need to make additional changes first), add the `no-auto-tag` label to the PR before merging. You can then manually create the tag later:

```bash
git tag v<version>
git push origin v<version>
```

#### 3. Automatic Tagging

When the version bump PR is merged, the **Auto Tag** workflow automatically:

- Detects that a version bump PR was merged (based on the `re/bump-for-*` branch name)
- Checks if the tag already exists (fails if it does)
- Creates an annotated tag `v<version>`
- Pushes the tag to GitHub

#### 4. Automatic Release Creation

When the tag is pushed, the **Auto Release** workflow automatically:

- Validates the tag format (must be `vX.Y.Z`)
- Checks if a release already exists (fails if it does)
- Generates release notes from merged PRs since the last release
- Creates a GitHub release with the generated notes

The release workflow (if configured) can then build and upload release artifacts.

## Requirements

### GitHub Secrets

The workflows require the following secret to be configured in your repository:

- `PAT_RELEASE_ENGINEER` - A GitHub Personal Access Token with the following permissions:
  - `contents: write` - for creating tags and releases
  - `pull-requests: write` - for creating pull requests

This token is used instead of the default `GITHUB_TOKEN` to ensure that the tag push triggers the release workflow (GitHub Actions tokens don't trigger other workflows by design).

## Manual Release Process

If you need to create a release manually (e.g., the automated workflow fails), follow these steps:

### 1. Update the Version

```bash
# Edit Cargo.toml and update the version number
vim Cargo.toml

# Update Cargo.lock
cargo check

# Commit the changes
git add Cargo.toml Cargo.lock
git commit -m "Bump version to X.Y.Z"
git push origin main
```

### 2. Create and Push the Tag

```bash
# Create an annotated tag
git tag -a vX.Y.Z -m "Release vX.Y.Z"

# Push the tag
git push origin vX.Y.Z
```

### 3. Create the Release

```bash
# Using GitHub CLI
gh release create vX.Y.Z \
  --title "vX.Y.Z" \
  --generate-notes
```

Or create the release manually through the GitHub web interface.
