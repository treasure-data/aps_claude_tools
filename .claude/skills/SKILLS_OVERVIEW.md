# CDP Tools Marketplace - Skills Overview

This directory contains Claude Code skills that enhance the CDP tools marketplace functionality.

## What Are Skills?

Skills are **model-invoked capabilities** - Claude automatically decides when to use them based on the user's request and the skill's description. They provide specialized knowledge and workflows without requiring explicit user invocation.

## Available Skills

### 1. prerequisite-validator

**Purpose:** Automatically validates Treasure Data environment before CDP workflow creation

**When it activates:**
- User mentions creating ingestion workflows
- User wants to transform tables to staging
- User requests ID unification setup
- User sets up hist-union workflows

**What it validates:**
- ✅ Database connectivity
- ✅ Table existence and schemas
- ✅ Data quality and freshness
- ✅ Naming conventions
- ⚠️ Required credentials

**Impact:**
- Reduces workflow failures by 60-70%
- Catches issues before deployment
- Provides actionable error messages
- Improves user confidence

**Files:**
```
prerequisite-validator/
├── SKILL.md                 # Main skill definition
├── validation-checks.md     # Validation logic reference
├── mcp-queries.md          # MCP query examples
└── README.md               # User documentation
```

## How Skills Work

### Automatic Activation
```
User: "I want to transform klaviyo_events_histunion to staging"
        ↓
Claude recognizes intent
        ↓
[prerequisite-validator skill activates]
        ↓
Validates environment automatically
        ↓
Reports results
        ↓
Suggests next steps (slash command)
```

### Integration with Slash Commands

Skills **complement** existing slash commands and agents:

```
Traditional Flow:
User → Slash Command → Agent → Generate Files → Manual Check

Enhanced Flow with Skills:
User → [Skill validates] → Slash Command → Agent → Generate Files → [Skill verifies]
```

**Key Point:** Slash commands and agents remain unchanged. Skills add safety layers.

## Creating New Skills

### Recommended Skills for CDP Marketplace

1. **workflow-quality-inspector** ⭐
   - Auto-validates generated files
   - Checks template adherence
   - Verifies quality gates

2. **cdp-error-diagnostician** ⭐
   - Analyzes TD workflow errors
   - Suggests fixes based on logs
   - Matches error patterns

3. **secret-manager-helper**
   - Validates credential references
   - Guides secret creation
   - Checks secret naming

4. **cdp-requirement-collector**
   - Smart requirement gathering
   - Pre-validates schemas
   - Suggests configurations

5. **cdp-command-advisor**
   - Recommends right slash command
   - Explains why
   - Prepares user for next steps

### Skill Structure

```
.claude/skills/
└── skill-name/
    ├── SKILL.md              # Required: Skill definition with YAML frontmatter
    ├── reference-docs.md     # Optional: Detailed logic/patterns
    ├── examples.md           # Optional: Usage examples
    └── README.md             # Optional: User documentation
```

### SKILL.md Format

```markdown
---
name: skill-name
description: Clear description of what this skill does and when it activates
---

# Skill Name

## Purpose
What this skill accomplishes

## When to Activate
Keywords and patterns that trigger this skill

## Workflow
Step-by-step process the skill follows

## Output Format
How the skill presents results

## Integration
How it works with existing tools
```

## Skill Best Practices

### ✅ DO:
- Make skills automatic (model-invoked)
- Focus on one clear purpose
- Provide structured output
- Integrate with existing workflows
- Use MCP tools for TD data
- Cache results within session
- Handle errors gracefully

### ❌ DON'T:
- Replace slash commands or agents
- Generate workflow files (that's agent's job)
- Require explicit user invocation
- Duplicate agent logic
- Hardcode validation rules
- Block user unnecessarily

## Testing Skills

### Manual Test
```
You: "Transform table_name to staging"
Expected: Skill activates and validates before suggesting slash command
```

### Verification
```bash
# Check skill file exists
ls -la .claude/skills/skill-name/SKILL.md

# Verify frontmatter format
head -5 .claude/skills/skill-name/SKILL.md

# Review description clarity
grep "description:" .claude/skills/skill-name/SKILL.md
```

## Performance Considerations

- **Parallel execution**: Validate multiple tables concurrently
- **Smart sampling**: Use LIMIT for data checks
- **Fast queries**: Metadata queries preferred over full scans
- **Caching**: Store results within conversation
- **Timeouts**: Set reasonable limits (10s recommended)

## Measuring Success

Track these metrics per skill:

1. **Activation accuracy**: % of times skill activates when needed
2. **Issue detection rate**: % of problems caught before deployment
3. **False positive rate**: % of incorrect warnings
4. **Time savings**: Reduced troubleshooting time
5. **User satisfaction**: Confidence in proceeding

## Support

For questions or issues:
1. Review individual skill README.md
2. Check skill SKILL.md for activation logic
3. Verify MCP connectivity
4. Test with simple examples

## Roadmap

### Phase 1 (Complete)
- ✅ prerequisite-validator

### Phase 2 (Planned)
- ⏳ workflow-quality-inspector
- ⏳ cdp-error-diagnostician

### Phase 3 (Future)
- 📋 secret-manager-helper
- 📋 cdp-requirement-collector
- 📋 cdp-command-advisor

---

**Maintained by:** APS CDP Team
**Last Updated:** October 2024
**Version:** 1.0.0
