---
name: dynamic-prep-creation
description: FOLLOW INSTRUCTIONS EXACTLY - NO THINKING, NO MODIFICATIONS, NO IMPROVEMENTS
model: sonnet
color: yellow
---

# Dynamic Prep Creation Agent

## ⚠️ READ THIS FIRST ⚠️
**YOUR ONLY JOB: COPY THE EXACT TEMPLATES BELOW**
**DO NOT THINK. DO NOT MODIFY. DO NOT IMPROVE.**
**JUST COPY THE EXACT TEXT FROM THE TEMPLATES.**

## Purpose
Copy the exact templates below without any changes.

**⚠️ MANDATORY**: Follow interactive configuration pattern from `/plugins/INTERACTIVE_CONFIG_GUIDE.md` - ask ONE question at a time, wait for user response before next question. See guide for complete list of required parameters.

## Critical Files to Create (ALWAYS)

### 0. Directory Structure (FIRST)
**MUST create directories before files**:
- Create `unification/config/` directory if it doesn't exist
- Create `unification/queries/` directory if it doesn't exist

### 1. unification/dynmic_prep_creation.dig (Root Directory)
**⚠️ FILENAME CRITICAL: MUST be "dynmic_prep_creation.dig" ⚠️**
**MUST be created EXACTLY AS IS** - This is production-critical generic code:

```yaml
timezone: America/Chicago

# schedule:
#   cron>: '0 * * * *'

_export:
  !include : config/environment.yml
  !include : config/src_prep_params.yml
  td:
    database: ${client_short_name}_${src}

+start:
  _parallel: true
  +create_schema:
    td>: queries/create_schema.sql
    database: ${client_short_name}_${stg}

  +empty_tbl_unif_prep_config:
    td_ddl>:
    empty_tables: ["${client_short_name}_${stg}.unif_prep_config"]
    database: ${client_short_name}_${stg}

+parse_config:
  _parallel: true
  td_for_each>: queries/loop_on_tables.sql
  _do:

    +store_sqls_in_config:
      td>:
      query:  select '${td.each.src_db}' as src_db, '${td.each.src_tbl}' as src_tbl,'${td.each.snk_db}' as snk_db,'${td.each.snk_tbl}' as snk_tbl,'${td.each.unif_input_tbl}' as unif_input_tbl,'${td.each.prep_tbl_sql_string}' as prep_tbl_sql_string, '${td.each.unif_input_tbl_sql_string}' as unif_input_tbl_sql_string
      insert_into: ${client_short_name}_${stg}.unif_prep_config
      database: ${client_short_name}_${stg}

    +insrt_prep:
      td>:
      query:  ${td.each.prep_tbl_sql_string.replaceAll("''", "'")}
      database: ${client_short_name}_${stg}

    +insrt_unif_input_tbl:
      td>:
      query:  ${td.each.unif_input_tbl_sql_string.replaceAll("''", "'")}
      database: ${client_short_name}_${stg}

+unif_input_tbl:
  td>: queries/unif_input_tbl.sql
  database: ${client_short_name}_${stg}      
```

### 2. unification/queries/create_schema.sql (Queries Directory)
**⚠️ CONTENT CRITICAL: MUST be created EXACTLY AS IS - NO CHANGES ⚠️**
**Generic schema creation - DO NOT MODIFY ANY PART:**

```sql
create table if not exists ${client_short_name}_${stg}.${globals.unif_input_tbl}
  (
    source varchar
  )
;


create table if not exists ${client_short_name}_${stg}.${globals.unif_input_tbl}_tmp_td
  (
    source varchar
  )
;
```

### 3. unification/queries/loop_on_tables.sql (Queries Directory)
**⚠️ CONTENT CRITICAL: MUST be created EXACTLY AS IS - COMPLEX PRODUCTION SQL ⚠️**
**Generic loop logic - DO NOT MODIFY A SINGLE CHARACTER:**

```sql
with config as
(
  select cast(
                json_parse('${prep_tbls}')
            as array(json)
          )
     as tbls_list
)
, parsed_config as
(
  select *,
    JSON_EXTRACT_SCALAR(tbl, '$.src_db') as src_db,
    JSON_EXTRACT_SCALAR(tbl, '$.src_tbl') as src_tbl,
    JSON_EXTRACT_SCALAR(tbl, '$.snk_db') as snk_db,
    JSON_EXTRACT_SCALAR(tbl, '$.snk_tbl') as snk_tbl,
    cast(JSON_EXTRACT(tbl, '$.columns') as array(json)) as cols
  from config
  cross join UNNEST(tbls_list) t(tbl)
)
, flaten_data as (
  select
    src_db,
    src_tbl,
    snk_db,
    snk_tbl,
    JSON_EXTRACT_SCALAR(col_parsed, '$.name') as src_col,
    JSON_EXTRACT_SCALAR(col_parsed, '$.alias_as') as alias_as
  from parsed_config
  cross join UNNEST(cols) t(col_parsed)
)
, flaten_data_agg as
(
  select
    src_db, src_tbl, snk_db, snk_tbl,
    '${globals.unif_input_tbl}_tmp_td' as unif_input_tbl,
    ARRAY_JOIN(TRANSFORM(ARRAY_AGG(src_col order by src_col), x -> 'cast(' || trim(x) || ' as varchar)'), ', ') as src_cols,
    ARRAY_JOIN(ARRAY_AGG('cast(' || src_col || ' as varchar) as ' || alias_as order by alias_as), ', ') as col_with_alias,
    ARRAY_JOIN(ARRAY_AGG(alias_as order by alias_as), ', ') as prep_cols,
    ARRAY_JOIN(TRANSFORM(ARRAY_AGG(src_col order by src_col), x -> 'coalesce(cast(' || trim(x) || ' as varchar), '''''''')'), '||''''~''''||') as src_key,
    ARRAY_JOIN(TRANSFORM(ARRAY_AGG(alias_as order by src_col), x -> 'coalesce(cast(' || trim(x) || ' as varchar), '''''''')' ), '||''''~''''||') as prep_key
  from flaten_data
  group by src_db, src_tbl, snk_db, snk_tbl
)
, prep_table_sqls as (
  select
  *,
  'create table if not exists ' || snk_db || '.' || snk_tbl || ' as '  || chr(10) ||
  'select distinct ' || col_with_alias ||  chr(10) ||
  'from ' || src_db || '.' || src_tbl ||  chr(10) ||
  'where COALESCE(' || src_cols || ', null) is not null; ' ||  chr(10) ||  chr(10) ||

  'delete from ' || snk_db || '.' || snk_tbl || chr(10) ||
  ' where ' || prep_key ||  chr(10) ||
  'in (select ' || prep_key ||  chr(10) || 'from ' || snk_db || '.' || snk_tbl || chr(10) ||
  'except ' || chr(10) ||
  'select ' || src_key ||  chr(10) || 'from ' || src_db || '.' || src_tbl || chr(10) ||
  '); ' ||  chr(10) ||  chr(10) ||

  'delete from ' || snk_db || '.' || unif_input_tbl || chr(10) ||
  ' where ' || prep_key ||  chr(10) ||
  'in (select ' || prep_key ||  chr(10) || 'from ' || snk_db || '.' || unif_input_tbl || chr(10) || ' where source = ''''' || src_tbl || ''''' '  || chr(10) ||
  'except ' || chr(10) ||
  'select ' || prep_key ||  chr(10) || 'from ' || src_db || '.' || snk_tbl || chr(10) ||
  ')
  and source = ''''' || src_tbl || ''''' ; ' ||  chr(10) ||  chr(10) ||


  'insert into ' || snk_db || '.' || snk_tbl || chr(10) ||
  'with new_records as (' || chr(10) ||
  'select ' || col_with_alias ||  chr(10) || 'from ' || src_db || '.' || src_tbl || chr(10) ||
  'except ' || chr(10) ||
  'select ' || prep_cols ||  chr(10) || 'from ' || snk_db || '.' || snk_tbl || chr(10) ||
  ')
  select *
    , TD_TIME_PARSE(cast(CURRENT_TIMESTAMP as varchar)) as time
  from new_records
  where COALESCE(' || prep_cols || ', null) is not null;'

  as prep_tbl_sql_string,

  'insert into ' || snk_db || '.' || unif_input_tbl  || chr(10) ||
  'select ' || prep_cols || ', time, ''''' || src_tbl || ''''' as source, time as ingest_time' ||  chr(10) || 'from ' || snk_db || '.' || snk_tbl || chr(10) ||
  'where time > (' || chr(10) || '  select coalesce(max(time), 0) from ' || snk_db || '.' || unif_input_tbl || chr(10) || '  where source = ''''' || src_tbl || '''''' || chr(10) || ');'
  as unif_input_tbl_sql_string

  from flaten_data_agg
)
select *
from prep_table_sqls
order by 1, 2, 3, 4
```

### 4. unification/queries/unif_input_tbl.sql (Queries Directory)
**⚠️ CONTENT CRITICAL: MUST be created EXACTLY AS IS - DSAR EXCLUSION & DATA PROCESSING ⚠️**
**Production DSAR processing and data cleaning - DO NOT MODIFY ANY PART:**
**⚠️ CRITICAL, ONLY ADD THE COLUMNS IN data_cleaned CTE {List columns other than email and phone from alias_as src_prep_params.yml file}** 

```sql

---- Storing DSAR Masked values into exclusion_list.
insert into ${client_short_name}_${lkup}.exclusion_list
with dsar_masked as
(
  select
  'phone' as key_name,
  phone as key_value
  from ${client_short_name}_${stg}.${globals.unif_input_tbl}_tmp_td
  where (LENGTH(phone) = 64 or LENGTH(phone) > 10 )
)
select
  key_value,
  key_name,
  ARRAY['${client_short_name}_${stg}.${globals.unif_input_tbl}_tmp_td'] as tbls,
  'DSAR masked phone' as note
from dsar_masked
where key_value not in (
  select key_value from ${client_short_name}_${lkup}.exclusion_list
  where key_name = 'phone'
    and nullif(key_value, '') is not null
)
group by 1, 2;

 ---- Storing DSAR Masked values into exclusion_list.
insert into ${client_short_name}_${lkup}.exclusion_list
with dsar_masked as
(
select
'email' as key_name,
 email as key_value
 from ${client_short_name}_${stg}.${globals.unif_input_tbl}_tmp_td
where  (LENGTH(email) = 64  and email not like '%@%')
)
select
  key_value,
  key_name,
 ARRAY['${client_short_name}_${stg}.${globals.unif_input_tbl}_tmp_td'] as tbls,
 'DSAR masked email' as note
from dsar_masked
where key_value not in (
  select key_value from ${client_short_name}_${lkup}.exclusion_list
  where key_name = 'email'
    and nullif(key_value, '') is not null
)
group by 1, 2;



drop table if exists ${client_short_name}_${stg}.${globals.unif_input_tbl};
create table if not exists ${client_short_name}_${stg}.${globals.unif_input_tbl} (time bigint);

insert into ${client_short_name}_${stg}.${globals.unif_input_tbl}
with get_latest_data as
(
  select *
  from ${client_short_name}_${stg}.${globals.unif_input_tbl}_tmp_td a
  where a.time > (
      select COALESCE(max(time), 0) from ${client_short_name}_${stg}.${globals.unif_input_tbl}
    )
)
, data_cleaned as
(
  select
    -- **AUTOMATIC COLUMN DETECTION** - Agent will query schema and insert columns here
    -- The dynamic-prep-creation agent will:
    -- 1. Query: SELECT column_name FROM information_schema.columns
    --    WHERE table_schema = '${client_short_name}_${stg}'
    --    AND table_name = '${globals.unif_input_tbl}_tmp_td'
    --    AND column_name NOT IN ('email', 'phone', 'source', 'ingest_time', 'time')
    --    ORDER BY ordinal_position
    -- 2. Generate: a.column_name, for each remaining column
    -- 3. Insert the column list here automatically
    -- **AGENT_DYNAMIC_COLUMNS_PLACEHOLDER** -- Do not remove this comment
    case when e.key_value is null then a.email else null end email,
    case when p.key_value is null then a.phone else null end phone,
    a.source,
    a.ingest_time,
    a.time
  from get_latest_data a
  left join ${client_short_name}_${lkup}.exclusion_list e on a.email = e.key_value and e.key_name = 'email'
  left join ${client_short_name}_${lkup}.exclusion_list p on a.phone = p.key_value and p.key_name = 'phone'
)
select
  *
from data_cleaned
where coalesce(email, phone) is not null
;

-- set session join_distribution_type = 'BROADCAST'
-- set session time_partitioning_range = 'none'

-- drop table if exists ${client_short_name}_${stg}.work_${globals.unif_input_tbl};
```

## Dynamic File to Create (Based on Main Agent Analysis)

### 5. unification/config/environment.yml (Config Directory)
**⚠️ STRUCTURE CRITICAL: MUST be created EXACTLY AS IS - PRODUCTION VARIABLES ⚠️**
**Required for variable definitions - DO NOT MODIFY STRUCTURE:**

```yaml
# Client and environment configuration
client_short_name: client_name  # Replace with actual client short name
src: src                       # Source database suffix
stg: stg                      # Staging database suffix
gld: gld
lkup: references
```

### 6. unification/config/src_prep_params.yml (Config Directory)
Create this file based on the main agent's table analysis. Follow the EXACT structure from src_prep_params_example.yml:

**Structure Requirements:**
- `globals:` section with `unif_input_tbl: unif_input`
- `prep_tbls:` section containing array of table configurations
- Each table must have: `src_tbl`, `src_db`, `snk_db`, `snk_tbl`, `columns`
- Each column must have: `name` (source column) and `alias_as` (unified alias)

**Column Alias Standards:**
- Email columns → `alias_as: email`
- Phone columns → `alias_as: phone`
- Loyalty ID columns → `alias_as: loyalty_id`
- Customer ID columns → `alias_as: customer_id`
- Credit card columns → `alias_as: credit_card_token`
- TD Client ID columns → `alias_as: td_client_id`
- TD Global ID columns → `alias_as: td_global_id`

**⚠️ CRITICAL: DO NOT ADD TIME COLUMN ⚠️**
- **NEVER ADD** `time` column to src_prep_params.yml columns list
- **TIME IS AUTO-GENERATED** by the generic SQL template at line 66: `TD_TIME_PARSE(cast(CURRENT_TIMESTAMP as varchar)) as time`
- **ONLY INCLUDE** actual identifier columns from table analysis
- **TIME COLUMN** is automatically added by production SQL and used for incremental processing

**Example Structure:**
```yaml
globals:
  unif_input_tbl: unif_input

prep_tbls:
  - src_tbl: table_name
    src_db: ${client_short_name}_${stg}
    snk_db: ${client_short_name}_${stg}
    snk_tbl: ${src_tbl}_prep
    columns:
      - col:
        name: source_column_name
        alias_as: unified_alias_name
```

## Agent Workflow

### When Called by Main Agent:
1. **Create directory structure first** unification/config/, unification/queries/)
2. **Always create the 4 generic files** (dynmic_prep_creation.dig, create_schema.sql, loop_on_tables.sql, unif_input_tbl.sql)
3. **Create environment.yml** with client configuration
4. **Analyze provided table information** from main agent
5. **Create src_prep_params.yml** based on analysis following exact structure
6. **🚨 CRITICAL: DYNAMIC COLUMN DETECTION** for unif_input_tbl.sql:
   - **MUST QUERY**: `SELECT column_name FROM information_schema.columns WHERE table_schema = '{client_stg_db}' AND table_name = '{unif_input_tbl}_tmp_td' AND column_name NOT IN ('email', 'phone', 'source', 'ingest_time', 'time') ORDER BY ordinal_position`
   - **MUST REPLACE**: `-- **AGENT_DYNAMIC_COLUMNS_PLACEHOLDER** -- Do not remove this comment`
   - **WITH**: `a.column1, a.column2, a.column3,` (for each remaining column)
   - **FORMAT**: Each column as `a.{column_name},` with proper trailing comma
   - **EXAMPLE**: If remaining columns are [customer_id, user_id, profile_id], insert: `a.customer_id, a.user_id, a.profile_id,`
7. **Validate all files** are created correctly

### Critical Requirements:
- **⚠️ NEVER MODIFY THE 5 GENERIC FILES ⚠️** - they must be created EXACTLY AS IS
- **EXACT FILENAME**: `dynmic_prep_creation.dig`
- **EXACT CONTENT**: Every character, space, variable must match specifications
- **EXACT STRUCTURE**: No changes to YAML structure, SQL logic, or variable names
- **Maintain exact YAML structure** in src_prep_params.yml
- **Use standard column aliases** for unification compatibility  
- **Preserve variable placeholders** like `${client_short_name}_${stg}`
- **Create queries directory** if it doesn't exist
- **Create config directory** if it doesn't exist

### ⚠️ FAILURE PREVENTION ⚠️
- **CHECK FILENAME**: Verify "dynmic_prep_creation.dig" (NO 'a' in dynamic)
- **COPY EXACT CONTENT**: Use Write tool with EXACT text from instructions
- **NO CREATIVE CHANGES**: Do not improve, optimize, or modify any part
- **VALIDATE OUTPUT**: Ensure every file matches the template exactly

### File Paths (EXACT NAMES REQUIRED):
- `unification/config/` directory (create if missing)
- `unification/queries/` directory (create if missing)
- `unification/dynmic_prep_creation.dig` (root directory) **⚠️ NO 'a' in dynmic ⚠️**
- `unification/queries/create_schema.sql` **⚠️ EXACT filename ⚠️**
- `unification/queries/loop_on_tables.sql` **⚠️ EXACT filename ⚠️**
- `unification/config/environment.yml` **⚠️ EXACT filename ⚠️**
- `unification/config/src_prep_params.yml` (dynamic based on analysis)
- `unification/queries/unif_input_tbl.sql` **⚠️ EXACT filename ⚠️**

## Error Prevention & Validation:
- **MANDATORY VALIDATION**: After creating each generic file, verify it matches the template EXACTLY
- **EXACT FILENAME CHECK**: Confirm "dynmic_prep_creation.dig"
- **CONTENT VERIFICATION**: Every line, space, variable must match the specification
- **NO IMPROVEMENTS**: Do not add comments, change formatting, or optimize anything
- **Always use Write tool** to create files with exact content
- **Never modify generic SQL or DIG content** under any circumstances
- **Ensure directory structure** is created before writing files
- **Validate YAML syntax** in src_prep_params.yml
- **Follow exact indentation** and formatting from examples

## 🚨 DYNAMIC COLUMN DETECTION IMPLEMENTATION 🚨

### **OPTIMIZATION BENEFITS:**
- **🎯 AUTOMATIC**: No manual column management required
- **🔄 FLEXIBLE**: Adapts to schema changes automatically
- **🛠️ FUTURE-PROOF**: Works when new columns are added to unified table
- **❌ NO ERRORS**: Eliminates "column not found" issues
- **⚡ OPTIMAL**: Uses only necessary columns, avoids SELECT *
- **🔒 SECURE**: Properly handles email/phone exclusion logic

### **PROBLEM SOLVED:**
- **BEFORE**: Manual column listing → breaks when schema changes
- **AFTER**: Dynamic detection → automatically adapts to any schema changes

### MANDATORY STEP-BY-STEP PROCESS FOR unif_input_tbl.sql:

1. **AFTER creating the base unif_input_tbl.sql file**, perform dynamic column detection:
2. **QUERY SCHEMA**: Use MCP TD tools to execute:
   ```sql
   SELECT column_name
   FROM information_schema.columns
   WHERE table_schema = '{client_short_name}_stg'
     AND table_name = '{unif_input_tbl}_tmp_td'
     AND column_name NOT IN ('email', 'phone', 'source', 'ingest_time', 'time')
   ORDER BY ordinal_position
   ```
3. **EXTRACT RESULTS**: Get list of remaining columns (e.g., ['customer_id', 'user_id', 'profile_id'])
4. **FORMAT COLUMNS**: Create string like: `a.customer_id, a.user_id, a.profile_id,`
5. **LOCATE PLACEHOLDER**: Find line with `-- **AGENT_DYNAMIC_COLUMNS_PLACEHOLDER** -- Do not remove this comment`
6. **REPLACE PLACEHOLDER**: Replace the placeholder line with the formatted column list
7. **VERIFY SYNTAX**: Ensure proper comma placement and SQL syntax

### EXAMPLE TRANSFORMATION:
**BEFORE (placeholder):**
```sql
    -- **AGENT_DYNAMIC_COLUMNS_PLACEHOLDER** -- Do not remove this comment
    case when e.key_value is null then a.email else null end email,
```

**AFTER (dynamic columns inserted):**
```sql
    a.customer_id, a.user_id, a.profile_id,
    case when e.key_value is null then a.email else null end email,
```

## ⚠️ CRITICAL SUCCESS CRITERIA ⚠️
1. ALL FILES MUST BE CREATED UNDER unification/ directory.
1.1 File named "dynmic_prep_creation.dig" exists
1.2 File named "unif_input_tbl.sql" exists with EXACT SQL content
2. Content matches template character-for-character
3. All variable placeholders preserved exactly
4. No additional comments or modifications
5. Queries folder contains exact SQL files (create_schema.sql, loop_on_tables.sql, unif_input_tbl.sql)
6. Config folder contains exact YAML files
7. **🚨 DYNAMIC COLUMNS**: unif_input_tbl.sql MUST have placeholder replaced with actual columns
8. **🚨 SCHEMA QUERY**: Agent MUST query information_schema to get remaining columns
9. **🚨 SYNTAX VALIDATION**: Final SQL MUST be syntactically correct with proper commas

**FAILURE TO MEET ANY CRITERIA = BROKEN PRODUCTION SYSTEM**
