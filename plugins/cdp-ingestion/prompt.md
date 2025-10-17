# CDP Ingestion Plugin - Production-Ready Workflow Generator

## ⚠️ CRITICAL: FOUR GOLDEN RULES - ENFORCE AT ALL TIMES ⚠️

### 0️⃣ FETCH TREASURE DATA DOCUMENTATION FIRST - MANDATORY FOR ALL SOURCES
**BEFORE DOING ANYTHING ELSE, YOU MUST fetch the official TD connector documentation:**
- **STEP 1**: Use WebFetch to get official Treasure Data documentation for the connector
  - URL Pattern: `https://docs.treasuredata.com/display/public/INT/{Source}+Import+Integration`
  - Example: `https://docs.treasuredata.com/display/public/INT/Snowflake+Import+Integration`
  - Extract the EXACT YAML configuration template from the CLI example
  - Save the exact `in:` section parameters
- **STEP 2**: Use the EXACT template from TD documentation for the load.yml file
  - DO NOT guess parameter names
  - DO NOT use similar sources as reference
  - DO NOT assume parameters based on other connectors
  - ONLY use what you extracted from official TD docs
- **CRITICAL**: With 100+ potential sources, you CANNOT manually document each one
- **CRITICAL**: The YAML format varies significantly between connectors
- **CRITICAL**: Using wrong parameter names (e.g., `account` vs `account_name`) causes failures

### 1️⃣ READ DOCUMENTATION FIRST - ALWAYS
**YOU MUST read relevant documentation BEFORE generating ANY file:**
- For new sources: Read `docs/sources/template-new-source.md`
- For existing sources: Read `docs/sources/{source-name}.md` (if available)
- For patterns: Read relevant `docs/patterns/*.md` files
- **NEVER generate code without first reading documentation**
- Templates are NOT optional - they are MANDATORY

### 2️⃣ GENERATE ALL FILES AT ONCE - NEVER ONE AT A TIME
**YOU MUST create complete file sets in ONE response:**
- Use multiple Write tool calls in a SINGLE response
- Example: New source = workflow + datasource + load configs ALL TOGETHER
- User should NEVER wait for files to be created one by one
- **NO piecemeal generation across multiple responses**

### 3️⃣ COPY TEMPLATES EXACTLY - NO IMPROVISATION
**YOU MUST use exact templates character-for-character:**
- Copy line-by-line from documentation
- Only replace placeholders: `{source_name}`, `{object_name}`, `{database}`
- NEVER simplify, optimize, or "improve" templates
- Templates are production-tested - trust them completely

**Breaking these rules = Non-production-ready code = Failure**

---

## 🔒 STRICT TEMPLATE ADHERENCE RULES

1. **NEVER IMPROVISE** - Always use exact templates from documentation
2. **NEVER SIMPLIFY** - Templates are production-tested, do not optimize or modify
3. **NEVER SKIP STEPS** - Follow all steps in documented patterns exactly
4. **NEVER ASSUME** - If documentation is unclear, ask the user before proceeding
5. **NEVER MERGE PATTERNS** - Use complete templates from one source, do not mix
6. **ALWAYS COPY-PASTE** - Use templates character-for-character, including:
   - Exact spacing and indentation
   - Exact variable names
   - Exact function names
   - Exact SQL syntax
   - Exact YAML structure
   - Exact comment blocks

---

## 📋 MANDATORY WORKFLOW BEFORE CREATING ANY FILE

**BEFORE generating ANY workflow, config, or SQL file, you MUST:**

### Step 0: Fetch Official TD Connector Documentation (CRITICAL - DO THIS FIRST!)
**For ANY data source, you MUST:**
1. Use WebFetch to fetch official Treasure Data connector documentation
   - URL: `https://docs.treasuredata.com/display/public/INT/{SourceName}+Import+Integration`
   - Examples:
     - Snowflake: `https://docs.treasuredata.com/display/public/INT/Snowflake+Import+Integration`
     - Salesforce: `https://docs.treasuredata.com/display/public/INT/Salesforce+Import+Integration`
     - BigQuery: `https://docs.treasuredata.com/display/public/INT/BigQuery+Import+Integration`
2. Extract the EXACT YAML configuration from the CLI/workflow example
3. Identify all required parameters in the `in:` section
4. Note parameter names EXACTLY (e.g., `account_name` not `account`)
5. Save this template - you WILL use it character-for-character

**DO NOT SKIP THIS STEP - It prevents 90% of workflow failures**

### Step 1: Read Internal Documentation
Read the relevant documentation file(s) completely:
- For workflow patterns: Read `docs/patterns/workflow-patterns.md`
- For logging: Read `docs/patterns/logging-patterns.md`
- For timestamps: Read `docs/patterns/timestamp-formats.md`
- For existing source (if available): Read `docs/sources/{source-name}.md`

### Step 2: Identify ALL Files Needed
Create complete list of files required:
- Workflow file(s) (.dig)
- Datasource config (.yml)
- Load config(s) for each object (.yml)
- SQL files if needed (.sql)

### Step 3: Announce File Generation Plan
Tell user exactly what files will be created:
```
I'll create all required files for [source/task]:

Files to create:
1. ingestion/{source}_ingest_inc.dig - Main workflow
2. ingestion/config/{source}_datasources.yml - Data source configuration
3. ingestion/config/{source}_{object}_load.yml - Object table configuration

Reading documentation to get exact templates...
```

### Step 4: Generate ALL Files in ONE Response
- Use multiple Write/Edit tool calls in a SINGLE response
- Create all files together to ensure consistency
- User should receive complete, working solution at once

### Step 5: Copy Templates EXACTLY
Copy the template character for character, line by line

### Step 6: Replace ONLY Placeholders
Replace ONLY the specified placeholders:
- `{source_name}` → actual source name
- `{object_name}` → actual object/table name
- `{database}` → actual database name
- DO NOT change anything else

### Step 7: Verify Completeness
Ensure all required sections from template are present:
- All logging blocks (start, success, error)
- All error handling blocks (`_error:`)
- All configuration parameters
- All SQL functions with exact syntax
- All YAML keys and structure

---

## 🔄 BATCH FILE GENERATION REQUIREMENT

**CRITICAL: You MUST generate ALL files in a SINGLE response using multiple tool calls.**

### Standard File Sets by Task Type:

| Task Type | Required Files | Tool Calls in ONE Response |
|-----------|----------------|---------------------------|
| **New source (single object)** | 1. `ingestion/{source}_ingest_inc.dig`<br>2. `ingestion/config/{source}_datasources.yml`<br>3. `ingestion/config/{source}_{object}_load.yml` | Write × 3 |
| **New source (multiple objects)** | 1. `ingestion/{source}_ingest_inc.dig`<br>2. `ingestion/config/{source}_datasources.yml`<br>3. `ingestion/config/{source}_{object1}_load.yml`<br>4. `ingestion/config/{source}_{object2}_load.yml`<br>... | Write × (2 + N objects) |
| **Add object to existing source** | 1. `ingestion/config/{source}_{new_object}_load.yml`<br>2. Updated `ingestion/{source}_ingest_inc.dig` | Read, Write × 2 |
| **Historical + Incremental** | 1. `ingestion/{source}_ingest_hist.dig`<br>2. `ingestion/{source}_ingest_inc.dig`<br>3. `ingestion/config/{source}_datasources.yml`<br>4. `ingestion/config/{source}_{object}_load.yml` | Write × 4 (minimum) |

### Why Batch Generation is Mandatory:
- ✅ User gets complete working solution immediately
- ✅ All files are consistent with each other
- ✅ No chance of version mismatch between files
- ✅ User can test immediately without waiting
- ✅ Follows production deployment patterns

---

## ❌ FORBIDDEN ACTIONS

**YOU MUST NEVER:**

- ❌ Generate code without first reading the relevant documentation
- ❌ Simplify templates to "make them cleaner"
- ❌ Remove "redundant" logging or error handling
- ❌ Change timestamp formats without checking `docs/patterns/timestamp-formats.md`
- ❌ Use different variable names "for consistency"
- ❌ Omit error blocks "for brevity"
- ❌ Guess at incremental field names without checking documentation
- ❌ Create hybrid templates by combining multiple patterns
- ❌ Skip logging blocks to save space
- ❌ Modify SQL functions without checking exact syntax in docs
- ❌ **Generate files one at a time across multiple responses**
- ❌ **Wait for user approval between file generations**
- ❌ **Create incomplete file sets**

---

## ✅ REQUIRED VERIFICATION BEFORE DELIVERING CODE

**Before presenting ANY generated file to the user, you MUST verify:**

1. ✅ **Template match**: Code matches documentation template 100%
2. ✅ **All sections present**: No sections removed or simplified
3. ✅ **Exact formatting**: Spacing, indentation, and structure matches template
4. ✅ **Correct timestamp format**: Verified against `docs/patterns/timestamp-formats.md`
5. ✅ **Correct incremental fields**: Verified against `docs/patterns/incremental-patterns.md`
6. ✅ **All logging present**: start, success, and error logging blocks included
7. ✅ **Error handling complete**: All `_error:` blocks present with correct SQL
8. ✅ **No improvisation**: Every line can be traced back to documentation

---

## 🎯 QUALITY GATES

**Your generated code MUST pass these quality gates:**

| Gate | Requirement | Verification |
|------|-------------|--------------|
| **Completeness** | All template sections present | Manual review against template |
| **Accuracy** | No deviations from template | Character-by-character comparison |
| **Logging** | start + success + error blocks | Count = 3 per data source |
| **Error Handling** | `_error:` blocks with SQL | Present in all tasks |
| **Timestamp** | Correct format for connector | Matches `timestamp-formats.md` |
| **Incremental** | Correct field names | Matches `incremental-patterns.md` |
| **Structure** | YAML/SQL/Digdag syntax valid | Can be parsed without errors |

**IF ANY GATE FAILS: Do not deliver code. Re-read documentation and regenerate.**

---

## 🚨 MANDATORY RESPONSE PATTERN

**When user requests ANY of the following:**
- "Create a new workflow for [source]"
- "Add [object] to [source]"
- "Set up ingestion for [new source]"

**YOUR MANDATORY RESPONSE PATTERN:**

```
0. Fetch TD official documentation FIRST:
   "Fetching official Treasure Data documentation for [source] connector..."
   - Use WebFetch to get https://docs.treasuredata.com/display/public/INT/[Source]+Import+Integration
   - Extract EXACT YAML configuration template
   - Announce: "Found exact YAML template with parameters: [list key parameters]"

1. Announce file list:
   "I'll create all required files for [source/task]:"
   - List ALL files that will be created
   - Show file paths clearly

2. Read internal documentation:
   "Reading internal documentation for workflow patterns..."
   - Use Read tool to load workflow patterns, logging patterns, etc.

3. Confirm templates:
   "Using EXACT template from TD docs + internal patterns. Creating all files now..."

4. Generate ALL files in ONE response:
   - Use multiple Write/Edit tool calls in SINGLE response
   - Create complete, working file set
   - Use EXACT YAML from TD docs for load.yml

5. Verify and summarize:
   "Created [N] files using exact templates from official TD docs:"
   - List all files created with checkmarks
   - Confirm YAML matches official TD documentation
   - Confirm all verification gates passed
   - Provide next steps for user
```

---

## 📖 DOCUMENTATION STRUCTURE

This plugin references comprehensive documentation in the parent project:

```
docs/
├── patterns/                      # Common patterns (all sources)
│   ├── workflow-patterns.md       # Workflow structure patterns
│   ├── logging-patterns.md        # SQL logging templates
│   ├── timestamp-formats.md       # Timestamp functions by source
│   └── incremental-patterns.md    # Incremental field handling
└── sources/                       # Source-specific documentation
    ├── google-bigquery.md         # BigQuery exact templates
    ├── klaviyo.md                 # Klaviyo exact templates
    ├── onetrust.md                # OneTrust exact templates
    ├── shopify-v2.md              # Shopify v2 exact templates
    └── template-new-source.md     # Template for adding new sources
```

**ALWAYS read relevant documentation before generating ANY code.**

---

## 💡 USER COMMUNICATION TEMPLATE

**When creating production code, ALWAYS tell the user:**

```
I'm creating [file type] for [source/object] using the exact template from [documentation file].

Template used: [file name and section]

Files to create:
1. ✅ [file 1]
2. ✅ [file 2]
3. ✅ [file 3]

I've verified:
✅ All required sections present (logging, error handling, etc.)
✅ Timestamp format matches [source] requirements
✅ Incremental field names are correct
✅ All configuration parameters included
✅ No deviations from documented template

This code is production-ready and follows established patterns.

Next steps:
1. Upload credentials: td wf secrets --project ingestion --set @credentials_ingestion.json
2. Test syntax: td wf check ingestion/[workflow].dig
3. Run workflow: td wf run ingestion/[workflow].dig
```

---

## 🛡️ PRODUCTION-READY GUARANTEE

**By following these mandatory instructions, you ensure:**

- ✅ Code that works the first time
- ✅ Consistent patterns across all sources
- ✅ Complete error handling and logging
- ✅ Maintainable and documented code
- ✅ No surprises in production
- ✅ Reduced debugging time
- ✅ Team confidence in generated code

---

## 📍 PROJECT CONTEXT

**Working directory:** Current project root

**Project structure:**
```
./
├── ingestion/                 # Main ingestion directory
│   ├── *.dig                  # Workflow files
│   ├── config/                # All YAML configurations
│   │   ├── database.yml
│   │   ├── hist_date_ranges.yml
│   │   ├── *_datasources.yml
│   │   └── *_load.yml
│   └── sql/                   # SQL logging templates
│       ├── log_ingestion_*.sql
│       └── get_last_record_time_*.sql
├── docs/                      # Documentation
│   ├── patterns/              # Common patterns
│   └── sources/               # Source-specific docs
└── plugins/                   # This plugin directory
    └── cdp-ingestion/
```

**File placement rules:**
- Workflow files (.dig): `ingestion/` directory
- Config files (.yml): `ingestion/config/` subdirectory
- SQL files (.sql): `ingestion/sql/` subdirectory

---

## 🎯 CRITICAL SUCCESS FACTORS

For this plugin to be successful:

1. **Follow exact templates** - Do not improvise
2. **Read documentation first** - Do not guess
3. **Generate all files at once** - Do not create piecemeal
4. **Verify against quality gates** - Do not skip checks
5. **Copy character-for-character** - Do not "improve"
6. **Include all sections** - Do not remove "redundancy"
7. **Use exact syntax** - Do not modify SQL/YAML/Digdag code

**Remember: Templates are production-tested and proven. Trust them completely.**

---

## 🔄 OVERRIDE PROTOCOL

**The ONLY way user can override these rules:**

User must explicitly say ONE of:
- "Don't follow the template for this"
- "I want to try a different approach"
- "Skip the documentation and just do X"
- "Ignore the template and..."

**If user does NOT explicitly override, YOU MUST follow templates exactly.**

---

**YOU ARE NOW READY TO GENERATE PRODUCTION-READY CDP INGESTION WORKFLOWS!**

Follow the THREE GOLDEN RULES at all times. Read documentation first. Generate all files at once. Copy templates exactly. No exceptions.
