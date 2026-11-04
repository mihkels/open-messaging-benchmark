#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Install mise if needed, then install the pinned tools and run the checks.
# See docs/guides/kubernetes-local.md.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$root"

manual_mise() {
  cat <<'EOF'

mise is not installed, and the automatic install failed. Install it yourself, then run this script again.

macOS with Homebrew:

  brew install mise
  echo 'eval "$(mise activate zsh)"' >> ~/.zshrc

Any platform, official installer:

  curl https://mise.run | sh
  echo 'eval "$(mise activate zsh)"' >> ~/.zshrc

Open a new shell so ~/.local/bin is on PATH. Instructions: https://mise.jdx.dev/getting-started.html
EOF
}

find_mise() {
  if command -v mise >/dev/null 2>&1; then
    command -v mise
    return 0
  fi
  if [ -x "${HOME}/.local/bin/mise" ]; then
    printf '%s\n' "${HOME}/.local/bin/mise"
    return 0
  fi
  if command -v brew >/dev/null 2>&1; then
    prefix=$(brew --prefix mise 2>/dev/null || true)
    if [ -n "${prefix}" ] && [ -x "${prefix}/bin/mise" ]; then
      printf '%s\n' "${prefix}/bin/mise"
      return 0
    fi
  fi
  return 1
}

install_mise() {
  if command -v brew >/dev/null 2>&1; then
    echo "Installing mise with Homebrew..."
    if brew install mise; then
      return 0
    fi
    echo "Homebrew install failed. Trying the official installer..."
  fi
  if ! command -v curl >/dev/null 2>&1; then
    echo "curl is not installed, so the official mise installer cannot run." >&2
    return 1
  fi
  echo "Installing mise from https://mise.run ..."
  curl -fsSL https://mise.run | sh
}

if ! mise_bin=$(find_mise); then
  if ! install_mise; then
    manual_mise
    exit 1
  fi
  if ! mise_bin=$(find_mise); then
    manual_mise
    exit 1
  fi
fi

export PATH="$(dirname "$mise_bin"):${PATH}"
echo "Using mise: $mise_bin"
"$mise_bin" trust
"$mise_bin" install
"$mise_bin" run check
