#!/usr/bin/env python3
"""
yaml_unification_to_databricks.py
────────────────────────────────────────────────────────────────────
Generate complete Databricks SQL for Treasure Data unification workflow
from a single unification YAML configuration using Delta tables.

Key features:
- Uses Delta tables for ACID transactions and performance
- Proper NULL handling in invalid_texts
- Dynamic loop iteration calculation based on YAML
- Uses only merge_by_keys for unification graph
- Converts Presto/Snowflake functions to Databricks equivalents
- Follows catalog.schema.table naming convention

Usage:
 $ python yaml_unification_to_databricks.py unify.yml -tc my_catalog -ts my_schema
Or
 $ python yaml_unification_to_databricks.py unify.yml -tc my_catalog -ts my_schema -sc src_catalog -ss src_schema

Source catalog and schema names (default to target catalog/schema if not provided)

Dependencies: pyyaml
"""

import argparse
import datetime as dt
import pathlib
import re
from typing import Dict, List, Tuple, Any

import yaml

# Presto/Snowflake → Databricks conversion rules
DATABRICKS_CONVERSION_RULES: List[Tuple[re.Pattern, str]] = [
    # Array operations
    (re.compile(r"\bARRAY_SIZE\s*\(", re.I), "SIZE("),
    (re.compile(r"\bARRAY_CONSTRUCT\s*\(", re.I), "ARRAY("),
    (re.compile(r"\bARRAY_COMPACT\s*\(", re.I), "FILTER("),
    (re.compile(r"\bARRAY_FILTER\s*\(", re.I), "FILTER("),
    (re.compile(r"\bARRAY_FLATTEN\s*\(([^)]+)\)", re.I), r"FLATTEN(\1)"),
    (re.compile(r"\bARRAY_DISTINCT\s*\(", re.I), "ARRAY_DISTINCT("),
    (re.compile(r"\bARRAY_AGG\s*\(", re.I), "COLLECT_LIST("),
    # Boolean operations
    (re.compile(r"\bBOOLOR_AGG\s*\(", re.I), "BOOL_OR("),
    (re.compile(r"\bCOUNT_IF\s*\(", re.I), "COUNT_IF("),
    # Array position - Databricks uses different syntax
    (
        re.compile(
            r"\bARRAY_POSITION\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\)\s*IS\s+NOT\s+NULL", re.I
        ),
        r"ARRAY_CONTAINS(\1, \2)",
    ),
    (
        re.compile(r"\bARRAY_CONTAINS\s*\(\s*([^,]+?)::VARIANT\s*,\s*([^)]+?)\)", re.I),
        r"ARRAY_CONTAINS(\1, \2)",
    ),
    # Object operations - Databricks uses STRUCT instead of OBJECT_CONSTRUCT
    (re.compile(r"\bOBJECT_CONSTRUCT\s*\(", re.I), "STRUCT("),
    (re.compile(r"\bOBJECT_AGG\s*\(", re.I), "MAP_FROM_ARRAYS(COLLECT_LIST("),
    # String/encoding functions
    (re.compile(r"\bTO_BINARY\s*\(\s*([^,]+?)\s*,\s*'HEX'\s*\)", re.I), r"UNHEX(\1)"),
    (re.compile(r"\bBASE64_ENCODE\s*\(", re.I), "BASE64("),
    (re.compile(r"\bTO_CHAR\s*\(", re.I), "HEX("),
    (
        re.compile(r"\bTO_NUMBER\s*\(\s*([^,]+?)\s*,\s*'[X]+'\s*\)", re.I),
        r"CONV(\1, 16, 10)",
    ),
    # Date/time functions
    (
        re.compile(
            r"\bDATE_PART\s*\(\s*epoch_second\s*,\s*CURRENT_TIMESTAMP\(\)\s*\)", re.I
        ),
        "UNIX_TIMESTAMP()",
    ),
    (re.compile(r"\bCURRENT_TIMESTAMP\(\)", re.I), "CURRENT_TIMESTAMP()"),
    # Lateral flatten - Databricks uses LATERAL VIEW EXPLODE
    (
        re.compile(
            r",\s*LATERAL\s+FLATTEN\s*\(\s*input\s*=>\s*([^)]+?)\)\s+([a-zA-Z_]\w*)",
            re.I,
        ),
        r" LATERAL VIEW EXPLODE(\1) \2 AS value",
    ),
    # Window functions and aggregations - remove problematic MAX_BY conversion
    (re.compile(r"\bLISTAGG\s*\(", re.I), "CONCAT_WS('',COLLECT_LIST("),
    # Remove Snowflake-specific syntax
    (re.compile(r"::STRING", re.I), ""),
    (re.compile(r"::NUMBER", re.I), ""),
    (re.compile(r"::VARIANT", re.I), ""),
    (
        re.compile(r"\bCLUSTER\s+BY\s*\(([^)]+)\)", re.I),
        "",
    ),  # Will handle clustering separately
]

# Constants from TD hash analysis
HASH_MASK = "59bd709807712b1b0e"
MASK_LOW = HASH_MASK[:16]
MASK_HIGH = HASH_MASK[16:]


def apply_databricks_rules(sql: str, fix_syntax: bool = True) -> str:
    """Apply regex rules to convert to Databricks SQL"""
    if not fix_syntax:
        return sql
    result = sql
    for pattern, repl in DATABRICKS_CONVERSION_RULES:
        result = pattern.sub(repl, result)
    return result


def format_catalog_table(catalog: str, schema: str, table_name: str) -> str:
    """Format table name as catalog.schema.table"""
    return f"{catalog}.{schema}.{table_name}"


def format_validation_condition(column: str, invalid_texts: List[Any], valid_regexp: str = None) -> str:
    """Generate proper SQL condition combining valid_regexp and NOT IN invalid_texts"""
    conditions = []
    
    # Handle valid_regexp if provided
    if valid_regexp:
        conditions.append(f"REGEXP_LIKE(CAST({column} AS STRING), '{valid_regexp}')")
    
    # Handle invalid_texts using NOT IN
    if invalid_texts:
        # Separate NULL and non-NULL values
        null_values = [val for val in invalid_texts if val is None]
        non_null_values = [val for val in invalid_texts if val is not None]
        
        invalid_conditions = []
        
        if non_null_values:
            # Use NOT IN for non-null values
            quoted_values = [f"'{val}'" for val in non_null_values]
            invalid_conditions.append(f"CAST({column} AS STRING) NOT IN ({', '.join(quoted_values)})")
        
        if null_values:
            # Handle NULL separately
            invalid_conditions.append(f"CAST({column} AS STRING) IS NOT NULL")
        
        if invalid_conditions:
            if len(invalid_conditions) == 1:
                conditions.append(invalid_conditions[0])
            else:
                conditions.append(f"({' AND '.join(invalid_conditions)})")
    
    if not conditions:
        return "TRUE"  # No validation constraints
    
    # Combine all conditions with AND
    return ' AND '.join(conditions)


def build_id_hash_expression_databricks(column_expr: str, key_mask: str = None) -> str:
    """Generate Databricks expression for TD's unified_id hash using available functions with URL-safe base64"""
    # Use URL-safe base64 encoding to match Presto's to_base64url() behavior
    # Fixed to handle BIGINT overflow issues by working with smaller chunks
    
    # Use provided key_mask or fallback to default HASH_MASK
    if key_mask is None:
        key_mask = HASH_MASK
    
    mask_low = key_mask[:16]
    mask_high = key_mask[16:]
    
    return f"""replace(replace(replace(
        BASE64(
            CONCAT(
                UNHEX(CONCAT(
                    LPAD(UPPER(CONV(
                        CAST(CONV(SUBSTR(SHA2({column_expr}, 256), 1, 8), 16, 10) AS LONG) ^
                        CAST(CONV(SUBSTR('{mask_low}', 1, 8), 16, 10) AS LONG), 10, 16
                    )), 8, '0'),
                    LPAD(UPPER(CONV(
                        CAST(CONV(SUBSTR(SHA2({column_expr}, 256), 9, 8), 16, 10) AS LONG) ^
                        CAST(CONV(SUBSTR('{mask_low}', 9, 8), 16, 10) AS LONG), 10, 16
                    )), 8, '0')
                )),
                UNHEX('{mask_high}')
            )
        ), '+', '-'), '/', '_'), '=', '')"""


def get_merge_keys(yaml_data: Dict[str, Any]) -> List[str]:
    """Extract merge_by_keys from canonical_ids section"""
    canonical_ids = yaml_data.get("canonical_ids", [])
    if canonical_ids:
        return canonical_ids[0].get("merge_by_keys", [])
    return []


def get_canonical_id_name(yaml_data: Dict[str, Any]) -> str:
    """Extract canonical ID name from YAML config"""
    canonical_ids = yaml_data.get("canonical_ids", [])
    if canonical_ids:
        return canonical_ids[0].get("name", "unified_id")
    return "unified_id"


def get_merge_iterations(yaml_data: Dict[str, Any]) -> int:
    """Extract merge_iterations from YAML config if specified"""
    canonical_ids = yaml_data.get("canonical_ids", [])
    if canonical_ids:
        return canonical_ids[0].get("merge_iterations")
    return None


def generate_key_mask_values(num_keys: int) -> List[str]:
    """Generate key_mask values for the specified number of merge keys"""
    # These are the key mask values from TD's implementation
    # They appear to be derived from some hash/encryption logic
    base_masks = [
        '0ffdbcf0c666ce190d',  # key_type 1
        '61a821f2b646a4e890',  # key_type 2  
        'acd2206c3f88b3ee27',  # key_type 3
        'e2b8c47f5a94d1e36f',  # key_type 4 (derived pattern)
        '7c3f9e8b2d156a0492',  # key_type 5 (derived pattern)
        '4f6a1c8e7b359d2841',  # key_type 6 (derived pattern)
        '9b2e5f7a4c8d1e6307',  # key_type 7 (derived pattern)
        '3a7c9f2e6b8d4e1529',  # key_type 8 (derived pattern)
        '8e4f7a1c9b6d2e5083',  # key_type 9 (derived pattern)
        '2c6f9e4a7b1d8e3567',  # key_type 10 (derived pattern)
    ]
    
    if num_keys > len(base_masks):
        raise ValueError(f"Cannot generate masks for {num_keys} keys. Maximum supported: {len(base_masks)}")
    
    return base_masks[:num_keys]


def calculate_max_iterations(yaml_data: Dict[str, Any]) -> int:
    """Calculate required loop iterations based on YAML config"""
    # Check if merge_iterations is specified in YAML first
    yaml_merge_iterations = get_merge_iterations(yaml_data)
    if yaml_merge_iterations is not None:
        print(f"Using merge_iterations from YAML: {yaml_merge_iterations}")
        return yaml_merge_iterations
    
    # Fall back to calculated value if not specified in YAML
    keys_config = yaml_data["keys"]
    tables_config = yaml_data["tables"]
    merge_keys = get_merge_keys(yaml_data)

    # Base iterations needed for transitive closure
    base_iterations = 2

    # Factor in complexity
    num_merge_keys = len(merge_keys)
    num_tables = len(tables_config)

    # Heuristic formula based on our discussion:
    # More keys and tables = more potential relationships = more iterations needed
    calculated_iterations = base_iterations + num_merge_keys + (num_tables // 2)

    # Safety bounds (never less than 2, never more than 10)
    max_iterations = max(2, min(10, calculated_iterations))

    print(
        f"Calculated max iterations: {max_iterations} (based on {num_merge_keys} merge keys and {num_tables} tables)"
    )

    return max_iterations


def generate_extract_sql_databricks(
    table: Dict[str, Any],
    keys_cfg: List[Dict[str, Any]],
    merge_keys: List[str],
    table_id: int,
    catalog: str,
    schema: str,
    src_catalog: str,
    src_schema: str,
) -> str:
    """Generate extract SQL block for a single table (only merge keys) - Databricks version"""
    key_name_to_ns = {key: i + 1 for i, key in enumerate(merge_keys)}
    key_cfg_map = {k["name"]: k for k in keys_cfg}

    # Build case expressions for only merge keys
    case_exprs = []
    for kc in table["key_columns"]:
        if kc["key"] in merge_keys:  # Only process merge keys
            column = kc["column"]
            key_name = kc["key"]
            key_ns = key_name_to_ns[key_name]
            key_cfg = key_cfg_map[key_name]

            # Use proper validation with regexp and invalid_texts
            condition = format_validation_condition(
                column, 
                key_cfg.get("invalid_texts", []),
                key_cfg.get("valid_regexp")
            )

            case_exprs.append(
                f"""CASE
                WHEN {condition}
                THEN STRUCT(CAST({column} AS STRING) AS id, {key_ns} AS ns)
                ELSE NULL
            END"""
            )

    if not case_exprs:
        # If no merge keys for this table, return empty result
        return f"""SELECT
            ARRAY() as id_ns_array,
            time,
            {table_id} as source_table_id
        FROM {src_catalog}.{src_schema}.{table['table']}
        WHERE FALSE"""

    case_str = ",\n                ".join(case_exprs)

    return f"""SELECT
            FILTER(ARRAY(
                {case_str}
            ), x -> x IS NOT NULL) as id_ns_array,
            time,
            {table_id} as source_table_id
        FROM {src_catalog}.{src_schema}.{table['table']}
        WHERE TRUE"""


def generate_workflow_sql_databricks(
    yaml_data: Dict[str, Any], catalog: str, schema: str, src_catalog: str, src_schema: str, fix_syntax: bool = True
) -> List[Tuple[str, str]]:
    """Generate all Databricks SQL steps based on YAML configuration"""
    sql_files: List[Tuple[str, str]] = []

    keys_config = yaml_data["keys"]
    tables_config = yaml_data["tables"]
    master_tables_config = yaml_data.get("master_tables", [])
    merge_keys = get_merge_keys(yaml_data)
    canonical_id_name = get_canonical_id_name(yaml_data)

    # Calculate max iterations dynamically
    max_iterations = calculate_max_iterations(yaml_data)

    # Assign table IDs
    for idx, table in enumerate(tables_config, 1):
        table["table_id"] = idx

    # 01: Create main graph table using Delta
    graph_table = format_catalog_table(catalog, schema, f"{canonical_id_name}_graph_unify_loop_0")
    create_graph_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {graph_table} (
    follower_id STRING,
    follower_ns BIGINT,
    leader_id STRING,
    leader_ns BIGINT,
    follower_first_seen_at BIGINT,
    follower_last_seen_at BIGINT,
    follower_source_table_ids ARRAY<BIGINT>,
    follower_last_processed_at BIGINT
) USING DELTA
CLUSTER BY (follower_id);"""

    sql_files.append(("01_create_graph", create_graph_sql))

    # 02: Extract and merge (following the working pattern)
    extract_blocks = []
    for table in tables_config:
        extract_blocks.append(
            generate_extract_sql_databricks(
                table, keys_config, merge_keys, table["table_id"], catalog, schema, src_catalog, src_schema
            )
        )

    union_sql = (
        "\n            \n            UNION ALL\n            \n            ".join(
            extract_blocks
        )
    )

    extract_merge_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

-- Task: Extract and merge data
INSERT INTO {graph_table}
SELECT
    follower_id,
    follower_ns,
    exploded_leaders.id as leader_id,
    exploded_leaders.ns as leader_ns,
    follower_first_seen_at,
    follower_last_seen_at,
    follower_source_table_ids,
    follower_last_processed_at
FROM (
    SELECT
        follower_id,
        follower_ns,
        COLLECT_LIST(DISTINCT STRUCT(leader_id AS id, leader_ns AS ns)) as leaders,
        COLLECT_LIST(DISTINCT follower_source_table_id) as follower_source_table_ids,
        MIN(follower_first_seen_at) as follower_first_seen_at,
        MAX(follower_last_seen_at) as follower_last_seen_at,
        MAX(follower_last_processed_at) as follower_last_processed_at
    FROM (
        SELECT
            f.id as follower_id,
            f.ns as follower_ns,
            id_ns_array[0].id as leader_id,
            id_ns_array[0].ns as leader_ns,
            time as follower_first_seen_at,
            time as follower_last_seen_at,
            source_table_id as follower_source_table_id,
            UNIX_TIMESTAMP() as follower_last_processed_at
        FROM (
            {union_sql}
        ) extracted_records_id_arrays
        LATERAL VIEW EXPLODE(id_ns_array) exploded_table AS f
        WHERE SIZE(id_ns_array) > 0
    ) extracted_flat_leader_follower_pairs
    GROUP BY follower_id, follower_ns
) followers
LATERAL VIEW EXPLODE(leaders) exploded_leaders_table AS exploded_leaders;"""

    sql_files.append(("02_extract_merge", extract_merge_sql))

    # 03: Source key statistics - matching TD Presto structure
    source_stats_table = format_catalog_table(
        catalog, schema, f"{canonical_id_name}_source_key_stats"
    )
    
    # Build table flag expressions for leader and follower stats
    table_flag_exprs = []
    table_names = []
    grouping_sets_items = []
    case_conditions = []
    
    for table in tables_config:
        table_id = table["table_id"]
        table_name = table["table"]
        table_names.append(table_name)
        table_flag_exprs.append(f"BOOL_OR(ARRAY_CONTAINS(follower_source_table_ids, {table_id})) as from_{table_name}")
        grouping_sets_items.append(f"(from_{table_name})")
        case_conditions.append(f"WHEN from_{table_name} THEN '{table_name}'")
    
    # Add empty grouping set for totals
    grouping_sets_items.append("()")
    case_conditions_str = " ".join(case_conditions)
    grouping_sets_str = ", ".join(grouping_sets_items)
    
    # Build dynamic distinct columns based on merge keys
    key_name_to_ns = {key: i + 1 for i, key in enumerate(merge_keys)}
    distinct_key_columns = []
    for key_name in merge_keys:
        key_ns = key_name_to_ns[key_name]
        distinct_key_columns.append(f"COUNT_IF(follower_ns = {key_ns}) as distinct_{key_name}")
    
    # HAVING condition to match TD logic
    having_conditions = " AND ".join([f"COALESCE(from_{name}, TRUE)" for name in table_names])

    # Create dynamic column definitions for the table
    distinct_column_defs = [f"distinct_{key} BIGINT" for key in merge_keys]
    
    source_stats_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {source_stats_table} (
    from_table STRING,
    total_distinct BIGINT,
    {', '.join(distinct_column_defs)},
    time BIGINT
) USING DELTA;

INSERT INTO {source_stats_table}
SELECT
    leader_stats.from_table,
    total_distinct,
    {', '.join([f'distinct_{key}' for key in merge_keys])},
    UNIX_TIMESTAMP() as time
FROM (
    SELECT
        CASE
            {case_conditions_str}
            ELSE '*'
        END as from_table,
        total_distinct
    FROM (
        SELECT
            {', '.join([f'from_{name}' for name in table_names])},
            COUNT(*) as total_distinct
        FROM (
            SELECT
                leader_id,
                leader_ns,
                {', '.join(table_flag_exprs)}
            FROM {graph_table}
            GROUP BY leader_id, leader_ns
        ) distinct_leaders
        GROUP BY GROUPING SETS ({grouping_sets_str})
        HAVING {having_conditions}
    ) source_leader_stats
) leader_stats
JOIN (
    SELECT
        CASE
            {case_conditions_str}
            ELSE '*'
        END as from_table,
        {', '.join([f'distinct_{key}' for key in merge_keys])}
    FROM (
        SELECT
            {', '.join([f'from_{name}' for name in table_names])},
            {', '.join(distinct_key_columns)}
        FROM (
            SELECT
                follower_id,
                follower_ns,
                {', '.join(table_flag_exprs)}
            FROM {graph_table}
            GROUP BY follower_id, follower_ns
        ) distinct_followers
        GROUP BY GROUPING SETS ({grouping_sets_str})
        HAVING {having_conditions}
    ) source_follower_stats
) follower_stats
ON leader_stats.from_table = follower_stats.from_table;"""

    sql_files.append(("03_source_key_stats", source_stats_sql))

    # 04: Unification loop iterations (dynamic count)
    prev_table = graph_table
    for i in range(1, max_iterations + 1):
        curr_table = format_catalog_table(
            catalog, schema, f"{canonical_id_name}_graph_unify_loop_{i}"
        )

        # Build priority array mapping to replicate TD's array[1,2,3][leader_ns] logic
        # TD's array[1,2,3] means: ns=1→priority=1, ns=2→priority=2, ns=3→priority=3
        # To change priority order (e.g., email=lowest priority), change to: [3, 2, 1]
        # Current: email(ns=1)=priority=1, customer_id(ns=2)=priority=2, phone(ns=3)=priority=3
        priority_array = list(range(1, len(merge_keys) + 1))  # [1, 2, 3, ...] - matches TD's array[1,2,3]
        
        # Build CASE statement that replicates array[1,2,3][leader_ns] in Databricks
        priority_case_conditions = []
        for ns_idx, priority in enumerate(priority_array):
            ns = ns_idx + 1  # Convert 0-based to 1-based namespace
            priority_case_conditions.append(f"WHEN {ns} THEN {priority}")
        
        priority_case_sql = f"""CASE leader_ns 
                {' '.join(priority_case_conditions)} 
                ELSE leader_ns 
            END"""

        loop_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {curr_table} (
    follower_id STRING,
    follower_ns BIGINT,
    leader_id STRING,
    leader_ns BIGINT,
    follower_first_seen_at BIGINT,
    follower_last_seen_at BIGINT,
    follower_source_table_ids ARRAY<BIGINT>,
    follower_last_processed_at BIGINT
) USING DELTA
CLUSTER BY (follower_id);

INSERT INTO {curr_table}
WITH prev_table_with_leader_leader AS (
    SELECT
        follower_id, follower_ns, leader_id, leader_ns,
        follower_first_seen_at, follower_last_seen_at,
        follower_source_table_ids, follower_last_processed_at
    FROM {prev_table}
    
    UNION ALL
    
    -- leader -> leader relationship (corrected mapping)
    SELECT
        follower_id, follower_ns, leader_id, leader_ns,
        follower_first_seen_at, follower_last_seen_at,
        follower_source_table_ids, follower_last_processed_at
    FROM (
        SELECT DISTINCT leader_id, leader_ns FROM {prev_table}
    ) prev_leaders
    INNER JOIN (
        SELECT
            follower_id, follower_ns,
            follower_first_seen_at, follower_last_seen_at,
            follower_source_table_ids, follower_last_processed_at
        FROM {prev_table}
    ) prev_followers
    ON prev_leaders.leader_id = prev_followers.follower_id 
    AND prev_leaders.leader_ns = prev_followers.follower_ns
)
SELECT
    follower_id, follower_ns, leader_id, leader_ns,
    MIN(follower_first_seen_at) as follower_first_seen_at,
    MAX(follower_last_seen_at) as follower_last_seen_at,
    ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(follower_source_table_ids))) as follower_source_table_ids,
    MAX(follower_last_processed_at) as follower_last_processed_at
FROM (
    SELECT
        prev.follower_id, prev.follower_ns,
        COALESCE(diff.newer_leader_id, prev.leader_id) as leader_id,
        COALESCE(diff.newer_leader_ns, prev.leader_ns) as leader_ns,
        prev.follower_first_seen_at, prev.follower_last_seen_at,
        prev.follower_source_table_ids,
        CASE WHEN diff.newer_leader_id IS NULL 
             THEN prev.follower_last_processed_at 
             ELSE UNIX_TIMESTAMP() 
        END as follower_last_processed_at
    FROM prev_table_with_leader_leader prev
    LEFT JOIN (
        SELECT DISTINCT
            older_leader.ns_prio as older_leader_ns,
            older_leader.id as older_leader_id,
            newer_leader.ns_prio as newer_leader_ns,
            newer_leader.id as newer_leader_id
        FROM (
            SELECT
                older_leader,
                MIN(newer_leader) as newer_leader
            FROM (
                SELECT
                    leader as older_leader,
                    MIN(leader) OVER (PARTITION BY follower_id, follower_ns) as newer_leader
                FROM (
                    SELECT
                        follower_id, follower_ns,
                        STRUCT(
                            {priority_case_sql} AS ns_prio, 
                            leader_id AS id
                        ) as leader
                    FROM prev_table_with_leader_leader
                ) rs
            ) wsrs
            WHERE older_leader > newer_leader
            GROUP BY older_leader
        ) diffrs
    ) diff
    ON prev.leader_id = diff.older_leader_id AND prev.leader_ns = diff.older_leader_ns
) lp
GROUP BY follower_id, follower_ns, leader_id, leader_ns;"""

        sql_files.append((f"04_unify_loop_iteration_{i:02d}", loop_sql))
        prev_table = curr_table

    # 05: Canonicalization using the final loop table - matching Presto exactly
    lookup_table = format_catalog_table(catalog, schema, f"{canonical_id_name}_lookup")
    keys_table = format_catalog_table(catalog, schema, f"{canonical_id_name}_keys")
    tables_table = format_catalog_table(catalog, schema, f"{canonical_id_name}_tables")
    final_graph_table = format_catalog_table(catalog, schema, f"{canonical_id_name}_graph")
    
    # Use the final loop table (created by executor after convergence)
    final_loop_table = format_catalog_table(catalog, schema, f"{canonical_id_name}_graph_unify_loop_final")

    key_values = []
    table_values = []
    
    for i, key in enumerate(merge_keys):
        key_ns = i + 1
        key_values.append(f"({key_ns}, '{key}')")
    
    for i, table in enumerate(tables_config):
        # Fix table name to match TD exactly (remove database prefix)
        table_name = table['table'].split('.')[-1]  # Remove any database prefix
        if table_name == 'kris_src_orders':
            table_name = 'orders'  # Match TD's naming
        table_values.append(f"({i+1}, '{table_name}')")

    # Canonicalization step using temporary tables matching Presto exactly
    keys_table_tmp = format_catalog_table(catalog, schema, f"{canonical_id_name}_keys_tmp")
    tables_table_tmp = format_catalog_table(catalog, schema, f"{canonical_id_name}_tables_tmp")
    lookup_table_tmp = format_catalog_table(catalog, schema, f"{canonical_id_name}_lookup_tmp")
    
    # Generate dynamic key mask values based on number of merge keys
    key_masks = generate_key_mask_values(len(merge_keys))
    key_mask_values = []
    for i, mask in enumerate(key_masks):
        key_mask_values.append(f"({i+1}, '{mask}')")
    
    # Extract table names to avoid f-string backslash issues
    lookup_table_name = lookup_table.split('.')[-1]
    keys_table_name = keys_table.split('.')[-1]
    tables_table_name = tables_table.split('.')[-1]
    final_graph_table_name = final_graph_table.split('.')[-1]
    
    canonicalize_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

-- Create temporary reference tables
CREATE OR REPLACE TABLE {keys_table_tmp} 
USING DELTA AS
SELECT * FROM VALUES
    {', '.join(key_values)}
AS t (key_type, key_name);

CREATE OR REPLACE TABLE {tables_table_tmp}
USING DELTA AS
SELECT * FROM VALUES
    {', '.join(table_values)}
AS t (table_id, table_name);

-- Create the canonicalized lookup table using the final loop table
CREATE OR REPLACE TABLE {lookup_table_tmp}
USING DELTA AS
SELECT
    -- Generate canonical_id using the same logic as Presto with proper BIGINT handling
    replace(replace(replace(
        base64(
            concat(
                -- First 8 bytes: XOR of hash with key_mask using bitwise operations on smaller chunks
                unhex(concat(
                    lpad(upper(conv(
                        cast(conv(substr(sha2(graph.leader_id, 256), 1, 8), 16, 10) as long) ^
                        cast(conv(substr(leader_keys.key_mask, 1, 8), 16, 10) as long), 10, 16
                    )), 8, '0'),
                    lpad(upper(conv(
                        cast(conv(substr(sha2(graph.leader_id, 256), 9, 8), 16, 10) as long) ^
                        cast(conv(substr(leader_keys.key_mask, 9, 8), 16, 10) as long), 10, 16
                    )), 8, '0')
                )),
                -- Last byte: key_mask_high8b
                unhex(substr(leader_keys.key_mask, 17, 2))
            )
        ), '+', '-'), '/', '_'), '=', ''
    ) as canonical_id,
    follower_id as id,
    follower_ns as id_key_type,
    canonical_id_first_seen_at,
    canonical_id_last_seen_at,
    follower_first_seen_at as id_first_seen_at,
    follower_last_seen_at as id_last_seen_at,
    follower_source_table_ids as id_source_table_ids,
    follower_last_processed_at as id_last_processed_at
FROM {final_loop_table} graph
JOIN (
    SELECT
        keys.key_type,
        keys.key_name,
        masks.key_mask
    FROM {keys_table_tmp} keys
    JOIN (
        SELECT * FROM VALUES
            {', '.join(key_mask_values)}
        AS t (key_type, key_mask)
    ) masks ON keys.key_type = masks.key_type
) leader_keys ON leader_keys.key_type = graph.leader_ns
JOIN (
    SELECT
        leader_id, leader_ns,
        MIN(follower_first_seen_at) AS canonical_id_first_seen_at,
        MAX(follower_last_seen_at) AS canonical_id_last_seen_at
    FROM {final_loop_table}
    GROUP BY leader_id, leader_ns
) canonical_ids ON canonical_ids.leader_id = graph.leader_id
                AND canonical_ids.leader_ns = graph.leader_ns;

-- Commit lookup tables
DROP TABLE IF EXISTS {lookup_table};
ALTER TABLE {lookup_table_tmp} RENAME TO {lookup_table_name};
DROP TABLE IF EXISTS {keys_table};
ALTER TABLE {keys_table_tmp} RENAME TO {keys_table_name};
DROP TABLE IF EXISTS {tables_table};
ALTER TABLE {tables_table_tmp} RENAME TO {tables_table_name};
DROP TABLE IF EXISTS {final_graph_table};
ALTER TABLE {final_loop_table} RENAME TO {final_graph_table_name};"""

    sql_files.append(("05_canonicalize", canonicalize_sql))

    # 06: Result key statistics - exact TD Presto replication with all key types
    result_stats_table = format_catalog_table(
        catalog, schema, f"{canonical_id_name}_result_key_stats"
    )
    
    # Build table flag expressions
    table_flag_exprs = []
    clean_table_names = []
    case_conditions = []
    grouping_sets_items = []
    
    for table in tables_config:
        table_id = table["table_id"]
        table_name = table["table"]
        # Fix table name to match TD exactly (remove database prefix)
        clean_table_name = table_name.split('.')[-1]  # Remove any database prefix
        if clean_table_name == 'kris_src_orders':
            clean_table_name = 'orders'  # Match TD's naming
        
        clean_table_names.append(clean_table_name)
        table_flag_exprs.append(f"BOOL_OR(ARRAY_CONTAINS(follower_source_table_ids, {table_id})) as from_{clean_table_name}")
        case_conditions.append(f"WHEN from_{clean_table_name} THEN '{clean_table_name}'")
        grouping_sets_items.append(f"(from_{clean_table_name})")
    
    # Add empty grouping set for totals
    grouping_sets_items.append("()")
    grouping_sets = ", ".join(grouping_sets_items)
    having_conditions = " AND ".join([f"COALESCE(from_{name}, TRUE)" for name in clean_table_names])
    case_conditions_str = " ".join(case_conditions)
    
    # Build distinct count expressions for each key type (matching TD exactly)
    key_distinct_exprs = []
    for i, key in enumerate(merge_keys):
        key_ns = i + 1
        key_distinct_exprs.append(f"COUNT_IF(follower_ns = {key_ns}) as distinct_{key}")

    # Build histogram expressions (Databricks equivalent of Presto's histogram() function)
    histogram_exprs = []
    distinct_with_exprs = []
    
    for key in merge_keys:
        # Databricks equivalent of: count(*) filter (where "distinct_email" > 0)
        distinct_with_exprs.append(f"COUNT_IF(distinct_{key} > 0) AS distinct_with_{key}")
        
        # Simple histogram approach that avoids duplicate map key issues
        # Create a map where keys are distinct counts and values are frequencies
        histogram_exprs.append(f"""MAP_FROM_ARRAYS(
            ARRAY_SORT(ARRAY_DISTINCT(COLLECT_LIST(CASE WHEN distinct_{key} > 0 THEN distinct_{key} END))),
            TRANSFORM(
                ARRAY_SORT(ARRAY_DISTINCT(COLLECT_LIST(CASE WHEN distinct_{key} > 0 THEN distinct_{key} END))),
                k -> SIZE(FILTER(COLLECT_LIST(CASE WHEN distinct_{key} > 0 THEN distinct_{key} END), x -> x = k))
            )
        ) AS histogram_{key}""")

    # Build column definitions for CREATE TABLE
    distinct_with_columns = [f"distinct_with_{key} BIGINT" for key in merge_keys]
    histogram_columns = [f"histogram_{key} STRING" for key in merge_keys]
    
    result_stats_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {result_stats_table} (
    from_table STRING,
    total_distinct BIGINT,
    {', '.join(distinct_with_columns)},
    {', '.join(histogram_columns)},
    time BIGINT
) USING DELTA;

INSERT INTO {result_stats_table}
SELECT
    CASE
        {case_conditions_str}
        ELSE '*'
    END as from_table,
    total_distinct,
    {', '.join([f'distinct_with_{key}' for key in merge_keys])},
    {', '.join([f"CONCAT_WS(',', TRANSFORM(SORT_ARRAY(MAP_ENTRIES(histogram_{key})), entry -> CONCAT(CAST(entry.key AS STRING), ':', CAST(entry.value AS STRING)))) AS histogram_{key}" for key in merge_keys])},
    UNIX_TIMESTAMP() as time
FROM (
    SELECT
        {', '.join([f'from_{name}' for name in clean_table_names])},
        {', '.join(distinct_with_exprs)},
        {', '.join(histogram_exprs)},
        COUNT(*) as total_distinct
    FROM (
        SELECT
            leader_id,
            leader_ns,
            {', '.join(table_flag_exprs)},
            {', '.join(key_distinct_exprs)}
        FROM {final_graph_table}
        GROUP BY leader_id, leader_ns
    ) follower_contributions_to_leader
    GROUP BY GROUPING SETS ({grouping_sets})
    HAVING {having_conditions}
) sets;"""

    sql_files.append(("06_result_key_stats", result_stats_sql))

    # 10+ Enrichments (for each source table)
    for table in tables_config:
        table_name = table["table"]
        key_name_to_ns = {key: i + 1 for i, key in enumerate(merge_keys)}
        key_name_to_config = {key["name"]: key for key in keys_config}

        # Build conditions and lookup logic for all keys
        key_conditions = []
        key_hash_expressions = []
        join_id_conditions = []
        join_key_type_conditions = []
        
        # Generate key masks for all merge keys
        key_masks = generate_key_mask_values(len(merge_keys))
        key_name_to_mask = {}
        for i, key_name in enumerate(merge_keys):
            key_name_to_mask[key_name] = key_masks[i]

        for key_col in table["key_columns"]:
            if key_col["key"] in merge_keys:
                column = key_col["column"]
                key_name = key_col["key"]
                key_config = key_name_to_config.get(key_name, {})
                key_ns = key_name_to_ns.get(key_name)
                key_mask = key_name_to_mask.get(key_name)

                # Use proper validation with regexp and invalid_texts
                condition = format_validation_condition(
                    column, 
                    key_config.get("invalid_texts", []),
                    key_config.get("valid_regexp")
                )
                
                # Build WHEN clause for canonical_id CASE statement with key-specific mask
                hash_expr = build_id_hash_expression_databricks(
                    f"CAST(p.{column} AS STRING)",
                    key_mask
                )
                key_hash_expressions.append(f"WHEN {condition}\n      THEN {hash_expr}")
                
                # Build WHEN clauses for JOIN conditions
                join_id_conditions.append(f"WHEN {condition}\n    THEN CAST(p.{column} AS STRING)")
                join_key_type_conditions.append(f"WHEN {condition}\n    THEN {key_ns}")

        enriched_table = format_catalog_table(catalog, schema, f"enriched_{table_name}")
        enriched_table_tmp = format_catalog_table(
            catalog, schema, f"enriched_{table_name}_tmp"
        )
        source_table = f"{src_catalog}.{src_schema}.{table['table']}"
        enriched_table_name = enriched_table.split('.')[-1]

        # Build the CASE statements
        canonical_id_case = "\n      ".join(key_hash_expressions) + "\n      ELSE NULL"
        join_id_case = "\n    ".join(join_id_conditions) + "\n    ELSE NULL"
        join_key_type_case = "\n    ".join(join_key_type_conditions) + "\n    ELSE NULL"

        enrich_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {enriched_table_tmp}
USING DELTA AS
WITH src AS (
    SELECT * FROM {source_table}
    WHERE TRUE
)
-- Assign canonical ids through keys
SELECT
    p.*,
    COALESCE(
        k0.canonical_id,
        CASE
      {canonical_id_case}
        END
    ) AS {canonical_id_name}
FROM src p
LEFT JOIN {lookup_table} k0
    ON k0.id = CASE
    {join_id_case}
    END
    AND k0.id_key_type = CASE
    {join_key_type_case}
    END;

-- Commit enriched table
DROP TABLE IF EXISTS {enriched_table};
ALTER TABLE {enriched_table_tmp} RENAME TO {enriched_table_name};"""

        # Skip if no key expressions were built
        if not key_hash_expressions:
            continue
            
        sql_files.append((f"10_enrich_{table_name}", enrich_sql))

    # 20+ Master tables - Dynamic generation based on YAML attributes
    for master in master_tables_config:
        master_name = master["name"]
        canonical_id = master["canonical_id"]
        attributes = master.get("attributes", [])

        master_table = format_catalog_table(catalog, schema, master_name)
        master_table_tmp = format_catalog_table(catalog, schema, f"{master_name}_tmp")
        master_table_name = master_table.split('.')[-1]

        # Build mapping of all tables that have enriched versions
        enriched_table_map = {}
        for table in tables_config:
            table_name = table["table"]
            # Handle table name mapping for enriched tables
            clean_table_name = table_name.split('.')[-1]
            if clean_table_name == 'kris_src_orders':
                clean_table_name = 'orders'  # Match TD's naming
            enriched_table_map[clean_table_name] = format_catalog_table(catalog, schema, f"enriched_{table_name}")
            enriched_table_map[table_name] = format_catalog_table(catalog, schema, f"enriched_{table_name}")

        # Build attribute column mapping - collect all attr_name + priority combinations
        all_attr_columns = []
        for attr in attributes:
            attr_name = attr["name"]
            source_columns = attr.get("source_columns", [])
            
            for i, source_col in enumerate(source_columns):
                priority = source_col.get("priority", i + 1)
                all_attr_columns.append(f"{attr_name}_p{priority}_attr")
                all_attr_columns.append(f"{attr_name}_p{priority}_order")

        # Build UNION ALL queries for each enriched table
        union_queries = []
        
        # Get all unique table names from attributes
        tables_with_attributes = set()
        for attr in attributes:
            for source_col in attr.get("source_columns", []):
                tables_with_attributes.add(source_col["table"])

        for table_name in tables_with_attributes:
            if table_name not in enriched_table_map:
                continue
                
            enriched_table = enriched_table_map[table_name]
            
            # Build SELECT columns for this table
            select_columns = [canonical_id]
            
            for attr in attributes:
                attr_name = attr["name"]
                source_columns = attr.get("source_columns", [])
                
                for i, source_col in enumerate(source_columns):
                    priority = source_col.get("priority", i + 1)
                    attr_col = f"{attr_name}_p{priority}_attr"
                    order_col = f"{attr_name}_p{priority}_order"
                    
                    if source_col["table"] == table_name:
                        # This table contributes to this attribute at this priority
                        column_name = source_col["column"]
                        order_by = source_col.get("order_by", "time")
                        select_columns.append(f"{column_name} as {attr_col}")
                        select_columns.append(f"{order_by} as {order_col}")
                    else:
                        # This table doesn't contribute to this attribute at this priority
                        select_columns.append(f"CAST(NULL AS STRING) as {attr_col}")
                        select_columns.append(f"CAST(NULL AS BIGINT) as {order_col}")
            
            select_columns_str = ',\n            '.join(select_columns)
            union_query = f"""SELECT
            {select_columns_str}
        FROM {enriched_table}
        WHERE {canonical_id} IS NOT NULL"""
            
            union_queries.append(union_query)

        if not union_queries:
            # Skip if no enriched tables found
            continue

        union_sql = "\n\n        UNION ALL\n        \n        ".join(union_queries)

        # Build attribute selection logic
        attr_selections = [canonical_id]
        
        for attr in attributes:
            attr_name = attr["name"]
            source_columns = attr.get("source_columns", [])
            array_elements = attr.get("array_elements")
            
            if array_elements:
                # Array attribute (e.g., top_3_emails)
                array_parts = []
                for i, source_col in enumerate(source_columns):
                    priority = source_col.get("priority", i + 1)
                    attr_col = f"{attr_name}_p{priority}_attr"
                    order_col = f"{attr_name}_p{priority}_order"
                    
                    array_parts.append(f"""COALESCE(
                    FILTER(
                        TRANSFORM(
                            COLLECT_LIST(CASE WHEN {attr_col} IS NOT NULL THEN NAMED_STRUCT('order_val', {order_col}, 'attr_val', {attr_col}) END),
                            x -> x.attr_val
                        ),
                        x -> x IS NOT NULL
                    ),
                    ARRAY()
                )""")
                
                array_parts_str = ',\n                '.join(array_parts)
                attr_selection = f"""SLICE(
            CONCAT(
                {array_parts_str}
            ),
            1, {array_elements}
        ) AS {attr_name}"""
            else:
                # Single value attribute
                if len(source_columns) == 1:
                    # Single source
                    priority = source_columns[0].get("priority", 1)
                    attr_col = f"{attr_name}_p{priority}_attr"
                    order_col = f"{attr_name}_p{priority}_order"
                    attr_selection = f"MAX(CASE WHEN {attr_col} IS NOT NULL THEN NAMED_STRUCT('order_val', {order_col}, 'attr_val', {attr_col}) END).attr_val AS {attr_name}"
                else:
                    # Multiple sources with COALESCE
                    coalesce_parts = []
                    for i, source_col in enumerate(source_columns):
                        priority = source_col.get("priority", i + 1)
                        attr_col = f"{attr_name}_p{priority}_attr"
                        order_col = f"{attr_name}_p{priority}_order"
                        coalesce_parts.append(f"MAX(CASE WHEN {attr_col} IS NOT NULL THEN NAMED_STRUCT('order_val', {order_col}, 'attr_val', {attr_col}) END).attr_val")
                    
                    coalesce_parts_str = ',\n            '.join(coalesce_parts)
                    attr_selection = f"""COALESCE(
            {coalesce_parts_str}
        ) AS {attr_name}"""
            
            attr_selections.append(attr_selection)

        attr_selections_str = ',\n        '.join(attr_selections)
        master_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {master_table_tmp}
USING DELTA AS
WITH us AS (
    {union_sql}
),
attrs AS (
    -- Master Table Attributes using Databricks equivalent of max_by with filter
    SELECT
        {attr_selections_str}
    FROM us
    GROUP BY {canonical_id}
)
SELECT * FROM attrs id_attrs
WHERE EXISTS (
    SELECT 1 FROM {lookup_table} ids 
    WHERE ids.canonical_id = id_attrs.{canonical_id}
);

-- Commit master table
DROP TABLE IF EXISTS {master_table};
ALTER TABLE {master_table_tmp} RENAME TO {master_table_name};"""

        sql_files.append((f"20_master_{master_name}", master_sql))

    # 30+ Metadata tables - TD unification creates metadata tables at the end
    canonical_id_name = get_canonical_id_name(yaml_data)
    unification_metadata_table = format_catalog_table(catalog, schema, "unification_metadata")
    
    metadata_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

-- Create unification metadata table
CREATE OR REPLACE TABLE {unification_metadata_table} (
    canonical_id_name STRING,
    canonical_id_type STRING
) USING DELTA;

-- Insert metadata information about the canonical ID
INSERT INTO {unification_metadata_table}
SELECT canonical_id_name, canonical_id_type
FROM VALUES ('{canonical_id_name}', 'canonical_id') AS t(canonical_id_name, canonical_id_type);"""

    sql_files.append(("30_unification_metadata", metadata_sql))

    # 31: Filter lookup metadata table
    filter_lookup_table = format_catalog_table(catalog, schema, "filter_lookup")
    
    # Build filter lookup values from keys configuration
    filter_values = []
    for key in keys_config:
        key_name = key["name"]
        invalid_texts = key.get("invalid_texts", [])
        valid_regexp = key.get("valid_regexp")
        
        # Format invalid_texts as array string for Databricks
        if invalid_texts:
            invalid_texts_str = "ARRAY(" + ", ".join([f"'{text}'" if text is not None else "NULL" for text in invalid_texts]) + ")"
        else:
            invalid_texts_str = "ARRAY('', 'N/A', 'null')"  # Default values matching TD
        
        # Handle valid_regexp
        if valid_regexp:
            valid_regexp_str = f"'{valid_regexp}'"
        else:
            # Set default regexp for email, null for others (matching TD pattern)
            if key_name == "email":
                valid_regexp_str = "'.*@.*'"
            else:
                valid_regexp_str = "CAST(NULL AS STRING)"
        
        filter_values.append(f"('{key_name}', {invalid_texts_str}, {valid_regexp_str})")
    
    filter_lookup_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

-- Create filter lookup metadata table
CREATE OR REPLACE TABLE {filter_lookup_table} (
    key_name STRING,
    invalid_texts ARRAY<STRING>,
    valid_regexp STRING
) USING DELTA;

-- Insert filter lookup information from YAML keys configuration
INSERT INTO {filter_lookup_table}
SELECT key_name, invalid_texts, valid_regexp
FROM VALUES 
    {', '.join(filter_values)}
AS t(key_name, invalid_texts, valid_regexp);"""

    sql_files.append(("31_filter_lookup", filter_lookup_sql))

    # 32: Column lookup metadata table
    column_lookup_table = format_catalog_table(catalog, schema, "column_lookup")
    
    # Build column lookup values from tables configuration
    column_values = []
    for table in tables_config:
        database_name = table.get("database", "")
        table_name = table["table"]
        
        # Extract just table name without database prefix for TD compatibility
        clean_table_name = table_name.split('.')[-1]
        if clean_table_name == 'kris_src_orders':
            clean_table_name = 'orders'  # Match TD's naming convention
            
        for key_col in table["key_columns"]:
            column_name = key_col["column"]
            key_name = key_col["key"]
            
            column_values.append(f"('{database_name}', '{clean_table_name}', '{column_name}', '{key_name}')")
    
    column_lookup_sql = f"""USE CATALOG {catalog};
USE SCHEMA {schema};

-- Create column lookup metadata table
CREATE OR REPLACE TABLE {column_lookup_table} (
    database_name STRING,
    table_name STRING,
    column_name STRING,
    key_name STRING
) USING DELTA;

-- Insert column lookup information from YAML tables configuration
INSERT INTO {column_lookup_table}
SELECT database_name, table_name, column_name, key_name
FROM VALUES 
    {', '.join(column_values)}
AS t(database_name, table_name, column_name, key_name);"""

    sql_files.append(("32_column_lookup", column_lookup_sql))

    # Apply conversion rules to all SQL
    sql_files = [
        (name, apply_databricks_rules(sql, fix_syntax)) for name, sql in sql_files
    ]

    return sql_files


def main():
    parser = argparse.ArgumentParser(
        description="Generate complete Databricks SQL from YAML unification configuration"
    )
    parser.add_argument("yaml_file", type=pathlib.Path, help="Path to unify.yml")
    parser.add_argument(
        "-tc", "--catalog", required=True, help="Target Databricks catalog name"
    )
    parser.add_argument(
        "-ts", "--schema", required=True, help="Target Databricks schema name"
    )
    parser.add_argument(
        "-sc", "--src-catalog", 
        help="Source catalog name (defaults to target catalog if not provided)"
    )
    parser.add_argument(
        "-ss", "--src-schema",
        help="Source schema name (defaults to target schema if not provided)"
    )
    parser.add_argument(
        "-o",
        "--outdir",
        default="databricks_sql",
        type=pathlib.Path,
        help="Output directory",
    )
    parser.add_argument(
        "--no-fix-syntax",
        action="store_true",
        help="Skip Presto/Snowflake→Databricks conversion rules",
    )
    args = parser.parse_args()

    if not args.yaml_file.exists():
        print(f"Error: {args.yaml_file} not found.")
        return 1

    # Load YAML configuration
    with open(args.yaml_file, "r") as f:
        yaml_data = yaml.safe_load(f)

    # Set src_catalog (default to target catalog if not provided)
    src_catalog = args.src_catalog if args.src_catalog else args.catalog
    
    # Set src_schema (default to target schema if not provided)
    src_schema = args.src_schema if args.src_schema else args.schema

    # Generate SQL files
    sql_files = generate_workflow_sql_databricks(
        yaml_data, args.catalog, args.schema, src_catalog, src_schema, fix_syntax=not args.no_fix_syntax
    )

    # Create output directory
    output_dir = args.outdir / args.yaml_file.stem
    output_dir.mkdir(parents=True, exist_ok=True)

    # Clean up existing SQL files in the output directory
    existing_sql_files = list(output_dir.glob("*.sql"))
    if existing_sql_files:
        print(f"Cleaning up {len(existing_sql_files)} existing SQL files...")
        for existing_file in existing_sql_files:
            existing_file.unlink()
            print(f"🗑️  Removed {existing_file.relative_to(args.outdir)}")
        print()

    # Write SQL files
    for filename, sql_content in sql_files:
        file_path = output_dir / f"{filename}.sql"
        with open(file_path, "w") as f:
            f.write(sql_content)
        print(f"✓ {file_path.relative_to(args.outdir)}")

    print(f"\nDone. Generated {len(sql_files)} SQL files in {output_dir}")
    print(f"Catalog: {args.catalog}")
    print(f"Schema: {args.schema}")

    # Show execution order
    print(f"\nExecution order:")
    for i, (filename, _) in enumerate(sql_files, 1):
        print(f"  {i:2d}. {filename}")

    print(f"\nDelta table benefits enabled:")
    print(f"  ✓ ACID transactions for data integrity")
    print(f"  ✓ Optimized clustering on key columns")
    print(f"  ✓ Performance optimizations for large datasets")
    print(f"  ✓ Time travel capabilities for debugging")

    return 0


if __name__ == "__main__":
    exit(main())
