#!/usr/bin/env bash
#
# setup-helloagents-snow.sh — Apply HelloAGENTS skills/hooks/scripts/templates
# to the snow-ai CLI (.snow/) configuration directory.
#
# Usage:
#   ./scripts/setup-helloagents-snow.sh          # install from global npm package
#   ./scripts/setup-helloagents-snow.sh /path    # install from custom source root
#
# This script is tracked in git because .snow/ is gitignored.
# Run it after cloning or pulling to set up the HelloAGENTS integration.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SNOW_ROOT="$PROJECT_ROOT/.snow"

# Determine helloagents source root
if [[ "${1:-}" != "" ]]; then
  HA_ROOT="$1"
else
  # Try npm global root
  NPM_GLOBAL_ROOT="$(npm root -g 2>/dev/null || true)"
  HA_ROOT="$NPM_GLOBAL_ROOT/helloagents"
  if [[ ! -d "$HA_ROOT" ]]; then
    # Try user-level npm-global
    HA_ROOT="$HOME/.npm-global/lib/node_modules/helloagents"
  fi
  # Try stable runtime root
  if [[ ! -d "$HA_ROOT" ]]; then
    HA_ROOT="$HOME/.helloagents/helloagents"
  fi
fi

if [[ ! -d "$HA_ROOT" ]]; then
  echo "ERROR: HelloAGENTS package not found."
  echo "Install it first:  npm install -g helloagents"
  echo "Or specify path:   $0 /path/to/helloagents"
  exit 1
fi

echo "HelloAGENTS source: $HA_ROOT"
echo "Snow-ai target:     $SNOW_ROOT"

# Create directory structure
mkdir -p "$SNOW_ROOT/skills" "$SNOW_ROOT/hooks" "$SNOW_ROOT/scripts" \
         "$SNOW_ROOT/templates" "$SNOW_ROOT/assets"

# Copy skills (helloagents, hello-*, commands/*, qa-review, _meta)
echo "Copying skills..."
cp -r "$HA_ROOT/skills/"* "$SNOW_ROOT/skills/"

# Copy hook definitions
echo "Copying hooks..."
cp "$HA_ROOT/hooks/"* "$SNOW_ROOT/hooks/" 2>/dev/null || true

# Copy scripts (notify.mjs, guard.mjs, ralph-loop.mjs, turn-state.mjs, etc.)
echo "Copying scripts..."
cp -r "$HA_ROOT/scripts/"* "$SNOW_ROOT/scripts/"

# Copy templates (plans, modules, PRD, etc.)
echo "Copying templates..."
cp -r "$HA_ROOT/templates/"* "$SNOW_ROOT/templates/"

# Copy bootstrap files (main system prompt injection)
echo "Copying bootstrap..."
cp "$HA_ROOT/bootstrap.md" "$SNOW_ROOT/bootstrap.md"
cp "$HA_ROOT/bootstrap-lite.md" "$SNOW_ROOT/bootstrap-lite.md"

# Copy assets (sounds, icons)
echo "Copying assets..."
cp -r "$HA_ROOT/assets/"* "$SNOW_ROOT/assets/"

# Copy plugin configs
echo "Copying plugin configs..."
cp -r "$HA_ROOT/.claude-plugin" "$SNOW_ROOT/.claude-plugin" 2>/dev/null || true
cp -r "$HA_ROOT/.codex-plugin" "$SNOW_ROOT/.codex-plugin" 2>/dev/null || true
cp "$HA_ROOT/gemini-extension.json" "$SNOW_ROOT/gemini-extension.json" 2>/dev/null || true

# Create snow-ai hooks configuration (adapted from Claude hooks)
echo "Creating snow-ai hooks configuration..."
cat > "$SNOW_ROOT/hooks/hooks-snow.json" << 'HOOKS_EOF'
{
  "hooks": {
    "SessionStart": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "node \"${SNOW_PLUGIN_ROOT}/scripts/notify.mjs\" inject --claude",
            "timeout": 10
          }
        ]
      }
    ],
    "UserPromptSubmit": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "node \"${SNOW_PLUGIN_ROOT}/scripts/notify.mjs\" route --claude",
            "timeout": 5
          }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "node \"${SNOW_PLUGIN_ROOT}/scripts/guard.mjs\"",
            "timeout": 5
          }
        ]
      },
      {
        "matcher": "Write|Edit|NotebookEdit",
        "hooks": [
          {
            "type": "command",
            "command": "node \"${SNOW_PLUGIN_ROOT}/scripts/guard.mjs\" pre-write",
            "timeout": 5
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Write|Edit|NotebookEdit",
        "hooks": [
          {
            "type": "command",
            "command": "node \"${SNOW_PLUGIN_ROOT}/scripts/guard.mjs\" post-write",
            "timeout": 5
          }
        ]
      }
    ],
    "PreCompact": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "node \"${SNOW_PLUGIN_ROOT}/scripts/notify.mjs\" pre-compact --claude",
            "timeout": 10
          }
        ]
      }
    ],
    "SubagentStop": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "node \"${SNOW_PLUGIN_ROOT}/scripts/ralph-loop.mjs\" subagent",
            "timeout": 120
          }
        ]
      }
    ],
    "Stop": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "node \"${SNOW_PLUGIN_ROOT}/scripts/notify.mjs\" stop --claude",
            "timeout": 120
          }
        ]
      }
    ]
  }
}
HOOKS_EOF

# Update settings.json with helloagents integration (preserve existing settings)
echo "Updating settings.json..."
if [[ -f "$SNOW_ROOT/settings.json" ]]; then
  # Use node to merge settings
  node -e "
    const fs = require('fs');
    const existing = JSON.parse(fs.readFileSync('$SNOW_ROOT/settings.json', 'utf8'));
    existing.helloagentsEnabled = true;
    existing.helloagentsBootstrap = '.snow/bootstrap.md';
    existing.helloagentsSkillsRoot = '.snow/skills';
    existing.helloagentsHooksFile = '.snow/hooks/hooks-snow.json';
    existing.helloagentsScriptsRoot = '.snow/scripts';
    existing.helloagentsTemplatesRoot = '.snow/templates';
    existing.helloagentsPluginRoot = '.snow';
    fs.writeFileSync('$SNOW_ROOT/settings.json', JSON.stringify(existing, null, 2) + '\n');
  "
else
  cat > "$SNOW_ROOT/settings.json" << 'SETTINGS_EOF'
{
  "yoloMode": true,
  "planMode": false,
  "vulnerabilityHuntingMode": false,
  "toolSearchEnabled": false,
  "hybridCompressEnabled": true,
  "teamMode": true,
  "ultraTodoEnabled": false,
  "helloagentsEnabled": true,
  "helloagentsBootstrap": ".snow/bootstrap.md",
  "helloagentsSkillsRoot": ".snow/skills",
  "helloagentsHooksFile": ".snow/hooks/hooks-snow.json",
  "helloagentsScriptsRoot": ".snow/scripts",
  "helloagentsTemplatesRoot": ".snow/templates",
  "helloagentsPluginRoot": ".snow",
  "goal": {
    "defaultTokenBudgetM": 2
  }
}
SETTINGS_EOF
fi

# Update permissions.json to include helloagents tools
echo "Updating permissions.json..."
if [[ -f "$SNOW_ROOT/permissions.json" ]]; then
  node -e "
    const fs = require('fs');
    const existing = JSON.parse(fs.readFileSync('$SNOW_ROOT/permissions.json', 'utf8'));
    if (!existing.alwaysApprovedTools) existing.alwaysApprovedTools = [];
    for (const tool of ['helloagents-turn-state', 'skill-execute']) {
      if (!existing.alwaysApprovedTools.includes(tool)) {
        existing.alwaysApprovedTools.push(tool);
      }
    }
    fs.writeFileSync('$SNOW_ROOT/permissions.json', JSON.stringify(existing, null, 2) + '\n');
  "
fi

# Create AGENTS.md symlink/copy for bootstrap content
echo "Creating AGENTS.md from bootstrap..."
cp "$SNOW_ROOT/bootstrap.md" "$PROJECT_ROOT/AGENTS.md" 2>/dev/null || true

echo ""
echo "✅ HelloAGENTS integration applied to snow-ai CLI (.snow/)"
echo ""
echo "Contents:"
echo "  Skills:    $(ls "$SNOW_ROOT/skills/" | wc -l) directories"
echo "  Scripts:   $(ls "$SNOW_ROOT/scripts/" | wc -l) files"
echo "  Hooks:     $(ls "$SNOW_ROOT/hooks/" | wc -l) files"
echo "  Templates: $(find "$SNOW_ROOT/templates/" -type f | wc -l) files"
echo "  Bootstrap: $SNOW_ROOT/bootstrap.md"
echo ""
echo "Settings updated: helloagentsEnabled=true"
echo "Hooks config:     .snow/hooks/hooks-snow.json"
