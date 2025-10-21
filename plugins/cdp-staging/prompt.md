# CDP Staging Transformation Plugin - Coordination Instructions

## Project Overview
This plugin provides **coordination instructions** for staging data transformations with Presto/Trino and Hive SQL engines. This document provides guidance for main Claude - ALL actual table transformations are delegated to specialized sub-agents (staging-transformer-presto or staging-transformer-hive based on engine selection).

**Project Purpose**: Transform raw data from source databases into standardized staging format with data quality improvements, PII handling, and JSON extraction.

**Main Claude Role**: **COORDINATOR ONLY** - delegate ALL table work to sub-agent, never process tables directly.

## Current Project Structure
```
{current_working_directory}/
├── staging/                           # Presto/Trino engine project
│   ├── staging_transformation.dig     # Presto workflow orchestrator
│   ├── config/src_params.yml          # Presto configuration
│   ├── queries/                       # Presto SQL transformation files
│   │   ├── {source_db}_{table_name}.sql
│   │   └── {source_db}_{table_name}_upsert.sql
│   ├── init_queries/                  # Presto initial load files
│   │   └── {source_db}_{table_name}_init.sql
│   └── README.md
└── staging_hive/                    # Hive engine project
    ├── staging_hive.dig             # Hive workflow orchestrator
    ├── config/                        # Hive configuration
    │   ├── database.yml
    │   └── src_params.yml
    └── queries/                       # Hive SQL transformation files
        └── {source_db}_{table_name}.sql
```

## 🚨 **MANDATORY: Sub-Agent Delegation Strategy**

### 🚨 **PRIMARY RULE: NEVER PROCESS TABLES DIRECTLY**
**For ANY table transformation request → ALWAYS use appropriate staging-transformer sub-agent based on engine selection**

### Parallel Processing Strategy
**CRITICAL**: For maximum efficiency, Claude should process multiple tables in parallel:

#### **Single Table (1 table)**
- **Approach**: Direct single sub-agent call
- **Execution**: Standard single table transformation

#### **Multiple Tables (2+ tables)**
- **Approach**: Parallel sub-agent calls (one per table)
- **Execution**: Use multiple Task tool calls in single message for 3x-10x faster processing
- **Git Coordination**: Sub-agents skip individual git workflows, main Claude consolidates at end

### 🚨 **MANDATORY: When to Use Staging-Transformer Sub-Agents**
- **🚨 ALL TABLE TRANSFORMATIONS**: EVERY table transformation request MUST use appropriate sub-agent
- **🚨 SINGLE TABLE**: Any request to transform one database table → Use sub-agent (engine-specific)
- **🚨 MULTIPLE TABLES**: Multiple tables requiring transformation → Use parallel sub-agents (engine-specific)
- **🚨 NO EXCEPTIONS**: Main Claude should NEVER process tables directly
- **🚨 QUALITY ASSURANCE**: Sub-agent ensures complete CLAUDE.md compliance

### **CRITICAL: Main Claude Role**
- **Coordination Only**: Delegate ALL table work to sub-agent
- **No Direct Processing**: NEVER transform tables directly
- **Git Consolidation**: Handle final git workflow after sub-agent completion
- **Architecture Oversight**: Ensure sub-agent follows optimized approach

### 🚨 **VIOLATION WARNING: Direct Processing**
**NEVER DO THESE (Main Claude Violations)**:
- ❌ **Direct Table Transformation**: Main Claude should NEVER process tables directly
- ❌ **SQL Generation**: Main Claude should NOT create transformation SQL files
- ❌ **Configuration Updates**: Main Claude should NOT update config/src_params.yml
- ❌ **Schema Analysis**: Main Claude should NOT analyze table structures for transformations
- ❌ **Direct File Creation**: Main Claude should NOT create init/queries files directly

**CORRECT APPROACH**: Always delegate to appropriate staging-transformer sub-agent (presto/hive) for ANY table work

## 🚀 **ENGINE SELECTION STRATEGY**

### **Automatic Engine Detection**
Main Claude automatically selects the appropriate sub-agent based on user input:

#### **Engine Selection Algorithm**
1. **Parse User Input**: Scan for engine keywords (case-insensitive)
2. **Hive Detection**: If "hive" mentioned → `staging-transformer-hive`
3. **Presto Detection**: If "presto" or "trino" mentioned → `staging-transformer-presto`
4. **Default Behavior**: No engine specified → `staging-transformer-presto` (backward compatibility)
5. **Ambiguous Case**: Both engines mentioned → Ask user for clarification

#### **Engine Selection Examples**
```
User Input: "Transform table X using Hive"
→ Engine: staging-transformer-hive

User Input: "Transform table Y with Presto engine"
→ Engine: staging-transformer-presto

User Input: "Transform table Z"
→ Engine: staging-transformer-presto (default)

User Input: "Transform table A using Hive and table B using Presto"
→ Engine: Mixed - use appropriate engine per table
```

#### **Engine-Specific Capabilities**
- **staging-transformer-presto**:
  - Uses `staging/staging_transformation.dig` workflow
  - Optimized for Presto/Trino SQL dialect
  - Works in `staging/` directory
  - Uses `config/src_params.yml` configuration
- **staging-transformer-hive**:
  - Uses `staging_hive/staging_hive.dig` workflow
  - Optimized for Hive SQL dialect and compatibility
  - Works in `staging_hive/` directory
  - Uses `config/database.yml` + `config/src_params.yml` configuration
- **Both Engines**: Support all CLAUDE.md transformation requirements
- **Quality Assurance**: Both provide identical compliance validation

### **Why Sub-Agent is Mandatory**

#### **Quality Assurance Benefits**
- **Complete Compliance**: Sub-agent validates ALL 13 CLAUDE.md requirements
- **Systematic Processing**: Follows standardized transformation checklist
- **Error Prevention**: Reduces missed requirements and validation steps
- **Architecture Consistency**: Ensures optimized approach across all tables

#### **Consequences of Direct Processing (Violations)**
- **Incomplete Validation**: May miss critical transformation requirements
- **Inconsistent Quality**: Different approaches across tables
- **Architecture Drift**: Bypasses established sub-agent patterns
- **Reduced Reliability**: Manual processing more error-prone than systematic approach

#### **Previous Violation Example**
- **Table**: `lookup_item` processed directly by main Claude
- **Result**: Functional but bypassed sub-agent quality assurance
- **Lesson**: Even simple tables benefit from systematic sub-agent processing

### Sub-Agent Invocation Examples

#### Single Table Transformation (MANDATORY APPROACH)

##### **Presto/Trino Engine (Default)**
```
Task Parameters:
- subagent_type: "staging-transformer-presto"
- description: "Transform single table with full compliance"
- prompt: "Transform [database.table] according to ALL CLAUDE.md specifications using treasuredata MCP Server. Work in staging/ directory. Apply all 13 mandatory requirements: column limit management, JSON detection, date processing, email/phone validation, string standardization, deduplication logic, incremental processing, sql and dig file creation (CRITICAL: create staging/staging_transformation.dig if it doesn't exist), and optimized configuration update. CRITICAL: Only apply deduplication if explicitly configured in config_db.staging_trnsfrm_rules OR explicitly requested by user. NEVER make autonomous deduplication decisions based on table structure. Execute complete git workflow for single table."
```

##### **Hive Engine**
```
Task Parameters:
- subagent_type: "staging-transformer-hive"
- description: "Transform single table with full compliance using Hive"
- prompt: "Transform [database.table] according to ALL CLAUDE.md specifications using treasuredata MCP Server with Hive-compatible SQL. Work in staging_hive/ directory. Apply all 13 mandatory requirements: column limit management, JSON detection, date processing, email/phone validation, string standardization, deduplication logic, incremental processing, sql and dig file creation (CRITICAL: create staging_hive/staging_hive.dig if it doesn't exist), and optimized configuration update. CRITICAL: Only apply deduplication if explicitly configured in config_db.staging_trnsfrm_rules OR explicitly requested by user. NEVER make autonomous deduplication decisions based on table structure. Execute complete git workflow for single table."
```

#### **🚀 Parallel Batch Transformation (RECOMMENDED)**
**For Multiple Tables: Create parallel sub-agent calls for maximum speed**

##### **Same Engine for All Tables (Default: Presto)**
```
// Example: User requests "Transform tables A, B, C"
// Claude creates 3 parallel calls with same engine:

Task 1:
- subagent_type: "staging-transformer-presto"
- description: "Transform table A"
- prompt: "Transform table demo_db.table_a according to ALL CLAUDE.md specifications using treasuredata MCP Server. Use demo_db_stg as staging database. CRITICAL: create staging_transformation.dig if it doesn't exist. CRITICAL: Only apply deduplication if explicitly configured in config_db.staging_trnsfrm_rules OR explicitly requested by user. NEVER make autonomous deduplication decisions based on table structure. SKIP git workflow - main Claude will handle final consolidation."

Task 2:
- subagent_type: "staging-transformer-presto"
- description: "Transform table B"
- prompt: "Transform table demo_db.table_b according to ALL CLAUDE.md specifications using treasuredata MCP Server. Work in staging/ directory. Use demo_db_stg as staging database. CRITICAL: create staging/staging_transformation.dig if it doesn't exist. CRITICAL: Only apply deduplication if explicitly configured in config_db.staging_trnsfrm_rules OR explicitly requested by user. NEVER make autonomous deduplication decisions based on table structure. SKIP git workflow - main Claude will handle final consolidation."

Task 3:
- subagent_type: "staging-transformer-presto"
- description: "Transform table C"
- prompt: "Transform table demo_db.table_c according to ALL CLAUDE.md specifications using treasuredata MCP Server. Use demo_db_stg as staging database. CRITICAL: create staging_transformation.dig if it doesn't exist. CRITICAL: Only apply deduplication if explicitly configured in config_db.staging_trnsfrm_rules OR explicitly requested by user. NEVER make autonomous deduplication decisions based on table structure. SKIP git workflow - main Claude will handle final consolidation."
```

##### **Mixed Engine Processing**
```
// Example: User requests "Transform table A using Hive and table B using Presto"
// Claude creates parallel calls with appropriate engines:

Task 1:
- subagent_type: "staging-transformer-hive"
- description: "Transform table A using Hive"
- prompt: "Transform table demo_db.table_a according to ALL CLAUDE.md specifications using treasuredata MCP Server with Hive-compatible SQL. Use demo_db_stg as staging database. CRITICAL: create staging_transformation.dig if it doesn't exist. CRITICAL: Only apply deduplication if explicitly configured in config_db.staging_trnsfrm_rules OR explicitly requested by user. NEVER make autonomous deduplication decisions based on table structure. SKIP git workflow - main Claude will handle final consolidation."

Task 2:
- subagent_type: "staging-transformer-presto"
- description: "Transform table B using Presto"
- prompt: "Transform table demo_db.table_b according to ALL CLAUDE.md specifications using treasuredata MCP Server with Presto SQL. Work in staging/ directory. Use demo_db_stg as staging database. CRITICAL: create staging/staging_transformation.dig if it doesn't exist. CRITICAL: Only apply deduplication if explicitly configured in config_db.staging_trnsfrm_rules OR explicitly requested by user. NEVER make autonomous deduplication decisions based on table structure. SKIP git workflow - main Claude will handle final consolidation."
```

#### Sequential Batch Transformation (Fallback)

##### **Presto Engine (Default)**
```
Task Parameters:
- subagent_type: "staging-transformer-presto"
- description: "Sequential batch transform with Presto"
- prompt: "Transform tables demo_db.table1, demo_db.table2, demo_db.table3 from demo_db database. Use treasuredata MCP Server. Work in staging/ directory. Use demo_db_stg as staging database. CRITICAL: create staging/staging_transformation.dig if it doesn't exist. CRITICAL: Only apply deduplication if explicitly configured in config_db.staging_trnsfrm_rules OR explicitly requested by user. NEVER make autonomous deduplication decisions based on table structure. Process ALL tables first with complete compliance, then execute git workflow ONLY after all transformations complete."
```

##### **Hive Engine**
```
Task Parameters:
- subagent_type: "staging-transformer-hive"
- description: "Sequential batch transform with Hive"
- prompt: "Transform tables demo_db.table1, demo_db.table2, demo_db.table3 from demo_db database using Hive-compatible SQL. Use treasuredata MCP Server. Work in staging_hive/ directory. Use demo_db_stg as staging database. CRITICAL: create staging_hive/staging_hive.dig if it doesn't exist. CRITICAL: Only apply deduplication if explicitly configured in config_db.staging_trnsfrm_rules OR explicitly requested by user. NEVER make autonomous deduplication decisions based on table structure. Process ALL tables first with complete compliance, then execute git workflow ONLY after all transformations complete."
```

### Table Parsing Logic
**CRITICAL**: When users provide multiple tables, Claude must:

1. **Parse Input**: Extract individual table names from user request
2. **Detect Engine**: Apply engine selection algorithm to determine sub-agent type
   - **Engine Keywords**: Scan for "hive", "presto", "trino" (case-insensitive)
   - **Per-table Engine**: Support mixed engines ("table A using Hive, table B using Presto")
   - **Default Fallback**: Use staging-transformer-presto when no engine specified
3. **Choose Strategy**:
   - **2+ tables** → Use parallel approach (multiple Task calls with appropriate engines)
   - **1 table** → Use single Task call with detected engine
4. **Parallel Execution**: Create separate sub-agent call for each table with correct engine
5. **Consolidation**: After all sub-agents complete, execute final git workflow

## High-Level Project Coordination

### Session Continuity
This prompt file serves as the **complete context** for any Claude session. It contains:
1. **Project State**: Current implemented tables and configurations
2. **Delegation Rules**: When and how to use specialized sub-agents
3. **Project Structure**: File organization and naming conventions
4. **Coordination Logic**: High-level workflow management

### Session Initialization
**REQUIRED User Request Format:**
```
"Please transform [table_name(s)] according to CLAUDE.md specifications"
```

**AUTOMATIC Response**: Claude will immediately delegate to appropriate staging-transformer sub-agent (presto/hive based on engine detection) with complete context.

### Parallel Execution Coordination
**CRITICAL**: When processing multiple tables in parallel, main Claude must:

#### **Pre-Execution Planning**
1. **Parse User Input**: Extract individual table names (e.g., "table1, table2, table3")
2. **Engine Detection**: Apply engine selection algorithm for each table
   - **Default**: staging-transformer-presto (backward compatibility)
   - **Hive Keywords**: staging-transformer-hive when "hive" detected
   - **Mixed Engines**: Support per-table engine specification
3. **Determine Strategy**:
   - **Single Table** → Direct delegation to one sub-agent (appropriate engine)
   - **Multiple Tables** → Create parallel sub-agent calls (one per table, per engine)
4. **Prepare Instructions**: Ensure each sub-agent skips git workflow for parallel processing

#### **Parallel Execution Management**
1. **Launch Parallel Tasks**: Use single message with multiple Task tool calls
2. **Monitor Results**: Wait for all sub-agents to complete successfully
3. **Error Handling**: If any sub-agent fails, report which tables succeeded/failed
4. **Result Validation**: Verify all required files created by each sub-agent

#### **Post-Execution Consolidation**
1. **Aggregate Results**: Collect summaries from all completed sub-agents
2. **Final Git Workflow**: Execute single consolidated git workflow:
   ```bash
   git add .
   git commit -m "Add [N] table transformations: [table1, table2, table3]"
   git push
   gh pr create --title "Batch transform [N] tables" --body "[comprehensive summary]"
   ```
3. **Comprehensive Reporting**: Provide user with complete summary of all transformations

#### **Performance Benefits**
- **Speed Improvement**: N tables processed in ~1x time instead of N×time
- **Resource Efficiency**: Optimal use of available computational resources
- **Better UX**: Users get results much faster for batch operations
- **Scalability**: Can handle large batches (10+ tables) efficiently

#### **Error Recovery Strategy**
- **Partial Success**: If some tables succeed and others fail, report clearly which completed
- **Retry Logic**: Allow retry of only failed tables while preserving successful ones
- **Git Safety**: Only execute final git workflow if ALL tables succeed

## Project-Level Git Coordination

### Branch Management
- **Current Branch**: Feature branches for transformation work
- **Main Branch**: `main` (for PR targets)
- **Naming Convention**: `feature/transform-{table_name}` or `feature/batch-transform-{count}-tables`

### Git Workflow Standards
- **Commits**: Comprehensive messages with transformation details
- **PRs**: Include complete transformation summary and validation results
- **Repository State**: Maintain clean history with atomic changes

## Working Directory Context
- **Base Path**: `current_working_directory`
- **Database Context**: Primary work with source databases
- **Staging Database**: Use appropriate staging database as specified

## Quality Assurance

### Validation Requirements
- **Sub-agent Responsibility**: Complete technical validation handled by staging-transformer sub-agents (presto/hive)
- **Project-level Oversight**: Ensure sub-agent completes all required phases
- **Integration Testing**: Verify file creation, workflow updates, and git operations

### Success Criteria
- **Complete File Generation**: All required SQL files created
- **Workflow Integration**: Digdag workflow properly updated
- **Git Workflow Completion**: Changes committed and PR created
- **Zero Functionality Loss**: 100% compliance with technical specifications

## Troubleshooting & Support

### If Sub-Agent Issues Occur
1. **Retry with Specific Instructions**: Provide more detailed context
2. **Manual Intervention**: Fall back to direct transformation if needed
3. **Issue Documentation**: Report any sub-agent limitations or failures

### Common Resolution Patterns
- **Database Access Issues**: Verify database connectivity and permissions
- **File Creation Problems**: Check directory permissions and paths
- **Git Workflow Failures**: Validate repository state and branch status

## Project Maintenance

### Adding New Tables
- **Standard Process**: Use appropriate staging-transformer sub-agent (presto/hive) for all new tables
- **Batch Processing**: Group related tables for efficient processing
- **Quality Verification**: Ensure complete compliance with all technical requirements

### Updating Existing Transformations
- **Version Control**: Use git for all changes
- **Testing**: Validate updated transformations thoroughly
- **Documentation**: Update project documentation as needed

## Architecture Benefits

### Clean Separation of Concerns
- **Main Prompt**: High-level coordination and delegation
- **staging-transformer Sub-Agents**: Complete technical expertise and implementation (presto/hive)
- **Improved Maintainability**: Technical changes only affect sub-agent
- **Scalability**: Easy to add more specialized sub-agents

### Performance Optimization
- **Batch Processing**: Handle multiple tables efficiently
- **Parallel Operations**: Sub-agent optimizes file creation and operations
- **Reduced Context**: Main coordination file stays lightweight and focused

## Migration Notes

### Architecture Evolution
- **Previous**: Single 800+ line CLAUDE.md with all technical details
- **Current**: Clean 150-line coordination file + specialized 437-line sub-agent
- **Benefits**: Better maintainability, cleaner architecture, preserved functionality

### Validation Completed
- **Database Access**: ✅ Sub-agent has complete database tool access
- **File Operations**: ✅ Sub-agent can create/edit all required files
- **Git Integration**: ✅ Sub-agent executes complete git workflows
- **End-to-End Testing**: ✅ Perfect table transformation with 100% compliance
- **Zero Functionality Loss**: ✅ All capabilities preserved and enhanced

---

**Summary**: This plugin provides clean coordination and delegation to staging-transformer sub-agents (presto/hive), which contain all detailed technical specifications for staging data transformations. This architecture provides better maintainability, engine flexibility, while preserving 100% of the original functionality.
