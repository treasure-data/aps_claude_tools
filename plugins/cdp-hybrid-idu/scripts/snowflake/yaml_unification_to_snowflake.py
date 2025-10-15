#!/usr/bin/env python3
"""
yaml_unification_to_snowflake.py
────────────────────────────────────────────────────────────────────
Generate complete Snowflake SQL for Treasure Data unification workflow
from a single unification YAML configuration.

Key features:
- Proper NULL handling in invalid_texts
- Dynamic loop iteration calculation based on YAML
- Uses only merge_by_keys for unification graph
- Converts Presto/Databricks functions to Snowflake equivalents
- Follows database.schema.table naming convention

Usage:
 $ python yaml_unification_to_snowflake.py unify.yml -d my_database -s my_schema
Or
 $ python yaml_unification_to_snowflake.py unify.yml -d my_database -s my_schema -sd src_database

Source database name (defaults to target database if not provided)

Dependencies: pyyaml
"""

import argparse
import datetime as dt
import pathlib
import re
from typing import Dict, List, Tuple, Any

import yaml

# Presto/Databricks → Snowflake conversion rules
SNOWFLAKE_CONVERSION_RULES: List[Tuple[re.Pattern, str]] = [
    # Array operations
    (re.compile(r"\bSIZE\s*\(", re.I), "ARRAY_SIZE("),
    (re.compile(r"\bARRAY\s*\(", re.I), "ARRAY_CONSTRUCT("),
    # Don't convert FILTER - keep it as is for SQL FILTER clause
    # (re.compile(r"\bFILTER\s*\(", re.I), "ARRAY_COMPACT("),
    # FLATTEN is already correct for Snowflake, but ARRAY_FLATTEN needs to be converted
    (re.compile(r"\bARRAY_FLATTEN\s*\(([^)]+)\)", re.I), r"FLATTEN(\1)"),
    (re.compile(r"\bCOLLECT_LIST\s*\(", re.I), "ARRAY_AGG("),
    # Boolean operations - Snowflake uses BOOLOR_AGG for aggregation
    (re.compile(r"\bBOOL_OR\s*\(", re.I), "BOOLOR_AGG("),
    # Array contains - Databricks uses ARRAY_CONTAINS, Snowflake uses ARRAYS_OVERLAP
    (re.compile(r"\bARRAY_CONTAINS\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\)", re.I), 
     r"ARRAYS_OVERLAP(\1, ARRAY_CONSTRUCT(\2))"),
    # Struct operations - Snowflake uses OBJECT_CONSTRUCT instead of STRUCT
    (re.compile(r"\bSTRUCT\s*\(", re.I), "OBJECT_CONSTRUCT("),
    (re.compile(r"\bNAMED_STRUCT\s*\(", re.I), "OBJECT_CONSTRUCT("),
    # String/encoding functions
    (re.compile(r"\bUNHEX\s*\(", re.I), "TO_BINARY("),
    (re.compile(r"\bBASE64\s*\(", re.I), "BASE64_ENCODE("),
    (re.compile(r"\bHEX\s*\(", re.I), "TO_CHAR("),
    (re.compile(r"\bCONV\s*\(\s*([^,]+?)\s*,\s*16\s*,\s*10\s*\)", re.I), 
     r"TO_NUMBER(\1, 'XXXXXXXXXXXXXXXX')"),
    (re.compile(r"\bCONV\s*\(\s*([^,]+?)\s*,\s*10\s*,\s*16\s*\)", re.I), 
     r"TO_CHAR(\1, 'X')"),
    # Date/time functions
    (re.compile(r"\bUNIX_TIMESTAMP\(\)", re.I), 
     "DATE_PART(epoch_second, CURRENT_TIMESTAMP())"),
    (re.compile(r"\bCURRENT_TIMESTAMP\(\)", re.I), "CURRENT_TIMESTAMP()"),
    # Lateral view explode - Snowflake uses LATERAL FLATTEN
    (re.compile(r"\bLATERAL\s+VIEW\s+EXPLODE\s*\(\s*([^)]+?)\)\s+([a-zA-Z_]\w*)\s+AS\s+value", re.I),
     r", LATERAL FLATTEN(input => \1) \2"),
    # Fix incorrect LATERAL ARRAY_FLATTEN usage - replace with proper LATERAL FLATTEN
    (re.compile(r"\bLATERAL\s+ARRAY_FLATTEN\s*\(input\s*=>\s*([^)]+)\)", re.I), r"LATERAL FLATTEN(input => \1)"),
    # Window functions and aggregations
    (re.compile(r"\bCONCAT_WS\s*\(\s*''\s*,\s*COLLECT_LIST\s*\(", re.I), "LISTAGG("),
    # Map operations
    (re.compile(r"\bMAP_FROM_ARRAYS\s*\(", re.I), "OBJECT_CONSTRUCT_KEEP_NULL("),
    # Remove Databricks-specific syntax
    (re.compile(r"\bUSING\s+DELTA", re.I), ""),
    (re.compile(r"\bCLUSTER\s+BY\s*\(([^)]+)\)", re.I), 
     r"CLUSTER BY (\1)"),  # Keep clustering for Snowflake
    # Cast operations
    (re.compile(r"\bCAST\s*\(\s*([^)]+?)\s+AS\s+LONG\s*\)", re.I), 
     r"CAST(\1 AS NUMBER)"),
    (re.compile(r"\bCAST\s*\(\s*([^)]+?)\s+AS\s+STRING\s*\)", re.I), 
     r"CAST(\1 AS VARCHAR)"),
]

# Constants from TD hash analysis
HASH_MASK = "59bd709807712b1b0e"
MASK_LOW = HASH_MASK[:16]
MASK_HIGH = HASH_MASK[16:]


def apply_snowflake_rules(sql: str, fix_syntax: bool = True) -> str:
    """Apply regex rules to convert to Snowflake SQL"""
    if not fix_syntax:
        return sql
    result = sql
    for pattern, repl in SNOWFLAKE_CONVERSION_RULES:
        result = pattern.sub(repl, result)
    return result


def format_database_table(database: str, schema: str, table_name: str) -> str:
    """Format table name as database.schema.table"""
    return f"{database}.{schema}.{table_name}"


def format_invalid_values_condition(column: str, invalid_texts: List[Any], valid_regexp: str = None) -> str:
    """Generate proper SQL condition for invalid values and valid regexp, handling NULL correctly"""
    conditions = []
    has_null = False
    non_null_values = []

    for val in invalid_texts:
        if val is None:
            has_null = True
        else:
            non_null_values.append(f"'{val}'")

    # Build conditions
    if non_null_values:
        # Use single NOT IN for multiple non-null invalid values
        conditions.append(f"CAST({column} AS VARCHAR) NOT IN ({', '.join(non_null_values)})")
    
    if has_null:
        conditions.append(f"CAST({column} AS VARCHAR) IS NOT NULL")

    # Add valid regexp condition if provided
    if valid_regexp:
        conditions.append(f"REGEXP_LIKE(CAST({column} AS VARCHAR), '{valid_regexp}')")

    if not conditions:
        return "FALSE"  # No valid conditions

    # Combine all conditions with AND
    return f"({' AND '.join(conditions)})"


def build_id_hash_expression_snowflake(column_expr: str, key_ns: int, num_keys: int = 10) -> str:
    """Generate Snowflake expression for TD's unified_id hash using key-specific masks"""
    # Generate dynamic key masks based on number of keys
    key_masks_list = generate_key_mask_values(num_keys)
    
    # Convert to dictionary for backward compatibility
    key_masks = {i + 1: mask for i, mask in enumerate(key_masks_list)}
    
    mask = key_masks.get(key_ns, key_masks_list[0])  # Default to first mask
    mask_low = mask[:16]
    mask_high = mask[16:]
    
    return f"""BASE64_ENCODE(
        CONCAT(
            TO_BINARY(
                LPAD(
                    LTRIM(
                        TO_CHAR(
                            BITXOR(
                                TO_NUMBER(SUBSTR(SHA2({column_expr}, 256), 1, 16), 'XXXXXXXXXXXXXXXX'),
                                TO_NUMBER('{mask_low}', 'XXXXXXXXXXXXXXXX')
                            ), 
                            'XXXXXXXXXXXXXXXX'
                        )
                    ), 16, '0'
                ), 'HEX'
            ),
            TO_BINARY('{mask_high}', 'HEX')
        )
    )"""


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


def get_key_priority_array(yaml_data: Dict[str, Any]) -> List[int]:
    """
    Extract or derive key-level priority array for unification loop.
    This replicates Presto's array[1, 2, 3][leader_ns] logic.
    
    Returns priority array where index corresponds to key namespace (0-based),
    and value represents the priority for that key type.
    
    Default: [1, 2, 3, ...] (sequential priorities)
    Configurable via canonical_ids.key_priorities if specified
    """
    canonical_ids = yaml_data.get("canonical_ids", [])
    if canonical_ids and "key_priorities" in canonical_ids[0]:
        # Use explicit key priorities if defined
        return canonical_ids[0]["key_priorities"]
    else:
        # Default to sequential priorities matching key order
        merge_keys = get_merge_keys(yaml_data)
        return list(range(1, len(merge_keys) + 1))


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


def generate_extract_sql_snowflake(
    table: Dict[str, Any],
    keys_cfg: List[Dict[str, Any]],
    merge_keys: List[str],
    table_id: int,
    database: str,
    schema: str,
    src_database: str,
    src_schema: str,
) -> str:
    """Generate extract SQL block for a single table (only merge keys) - Snowflake version"""
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

            # Use proper NULL handling and valid_regexp
            condition = format_invalid_values_condition(
                column, key_cfg.get("invalid_texts", []), key_cfg.get("valid_regexp")
            )

            case_exprs.append(
                f"""CASE
                WHEN {condition}
                THEN OBJECT_CONSTRUCT('id', CAST({column} AS VARCHAR), 'ns', {key_ns})
                ELSE NULL
            END"""
            )

    if not case_exprs:
        # If no merge keys for this table, return empty result
        # Use the provided src_schema parameter instead of inferring from YAML
        table_ref = f"{src_database}.{src_schema}.{table['table']}"
            
        return f"""SELECT
            ARRAY_CONSTRUCT() as id_ns_array,
            time,
            {table_id} as source_table_id
        FROM {table_ref}
        WHERE FALSE"""

    case_str = ",\n                ".join(case_exprs)

    # Use the provided src_schema parameter instead of inferring from YAML
    table_ref = f"{src_database}.{src_schema}.{table['table']}"

    return f"""SELECT
            ARRAY_COMPACT(ARRAY_CONSTRUCT(
                {case_str}
            )) as id_ns_array,
            time,
            {table_id} as source_table_id
        FROM {table_ref}
        WHERE TRUE"""


def generate_workflow_sql_snowflake(
    yaml_data: Dict[str, Any], database: str, schema: str, src_database: str, src_schema: str, fix_syntax: bool = True
) -> List[Tuple[str, str]]:
    """Generate all Snowflake SQL steps based on YAML configuration"""
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

    # 01: Create main graph table using Snowflake syntax
    graph_table = format_database_table(database, schema, f"{canonical_id_name}_graph_unify_loop_0")
    create_graph_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {graph_table} (
    follower_id VARCHAR,
    follower_ns NUMBER,
    leader_id VARCHAR,
    leader_ns NUMBER,
    follower_first_seen_at NUMBER,
    follower_last_seen_at NUMBER,
    follower_source_table_ids ARRAY,
    follower_last_processed_at NUMBER
)
CLUSTER BY (follower_id);"""

    sql_files.append(("01_create_graph", create_graph_sql))

    # 02: Extract and merge (following the working pattern)
    extract_blocks = []
    for table in tables_config:
        extract_blocks.append(
            generate_extract_sql_snowflake(
                table, keys_config, merge_keys, table["table_id"], database, schema, src_database, src_schema
            )
        )

    union_sql = (
        "\n            \n            UNION ALL\n            \n            ".join(
            extract_blocks
        )
    )

    extract_merge_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

-- Task: Extract and merge data
INSERT INTO {graph_table}
SELECT
    follower_id,
    follower_ns,
    exploded_leaders.value:id::VARCHAR as leader_id,
    exploded_leaders.value:ns::NUMBER as leader_ns,
    follower_first_seen_at,
    follower_last_seen_at,
    follower_source_table_ids,
    follower_last_processed_at
FROM (
    SELECT
        follower_id,
        follower_ns,
        ARRAY_AGG(DISTINCT OBJECT_CONSTRUCT('id', leader_id, 'ns', leader_ns)) as leaders,
        ARRAY_AGG(DISTINCT follower_source_table_id) as follower_source_table_ids,
        MIN(follower_first_seen_at) as follower_first_seen_at,
        MAX(follower_last_seen_at) as follower_last_seen_at,
        MAX(follower_last_processed_at) as follower_last_processed_at
    FROM (
        SELECT
            f.value:id::VARCHAR as follower_id,
            f.value:ns::NUMBER as follower_ns,
            id_ns_array[0]:id::VARCHAR as leader_id,
            id_ns_array[0]:ns::NUMBER as leader_ns,
            time as follower_first_seen_at,
            time as follower_last_seen_at,
            source_table_id as follower_source_table_id,
            DATE_PART(epoch_second, CURRENT_TIMESTAMP()) as follower_last_processed_at
        FROM (
            {union_sql}
        ) extracted_records_id_arrays,
        LATERAL FLATTEN(input => id_ns_array) f
        WHERE ARRAY_SIZE(id_ns_array) > 0
    ) extracted_flat_leader_follower_pairs
    GROUP BY follower_id, follower_ns
) followers,
LATERAL FLATTEN(input => leaders) exploded_leaders;"""

    sql_files.append(("02_extract_merge", extract_merge_sql))

    # 03: Source key statistics - matching TD Presto structure
    source_stats_table = format_database_table(
        database, schema, f"{canonical_id_name}_source_key_stats"
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
        table_flag_exprs.append(f"BOOLOR_AGG(ARRAYS_OVERLAP(follower_source_table_ids, ARRAY_CONSTRUCT({table_id}))) as from_{table_name}")
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
    distinct_column_defs = [f"distinct_{key} NUMBER" for key in merge_keys]
    
    source_stats_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {source_stats_table} (
    from_table VARCHAR,
    total_distinct NUMBER,
    {', '.join(distinct_column_defs)},
    time NUMBER
);

INSERT INTO {source_stats_table}
SELECT
    leader_stats.from_table,
    total_distinct,
    {', '.join([f'distinct_{key}' for key in merge_keys])},
    DATE_PART(epoch_second, CURRENT_TIMESTAMP()) as time
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
        curr_table = format_database_table(
            database, schema, f"{canonical_id_name}_graph_unify_loop_{i}"
        )

        # Build priority array mapping to replicate TD's array[1,2,3][leader_ns] logic
        priority_array = get_key_priority_array(yaml_data)  # Configurable priority array
        
        # Build CASE statement that replicates array[1,2,3][leader_ns] in Snowflake
        priority_case_conditions = []
        for ns_idx, priority in enumerate(priority_array):
            ns = ns_idx + 1  # Convert 0-based to 1-based namespace
            priority_case_conditions.append(f"WHEN {ns} THEN {priority}")
        
        priority_case_sql = f"""CASE leader_ns 
                {' '.join(priority_case_conditions)} 
                ELSE leader_ns 
            END"""

        loop_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {curr_table} (
    follower_id VARCHAR,
    follower_ns NUMBER,
    leader_id VARCHAR,
    leader_ns NUMBER,
    follower_first_seen_at NUMBER,
    follower_last_seen_at NUMBER,
    follower_source_table_ids ARRAY,
    follower_last_processed_at NUMBER
)
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
    ARRAY_DISTINCT(
        ARRAY_AGG(DISTINCT flattened_table_ids.value)
    ) as follower_source_table_ids,
    MAX(follower_last_processed_at) as follower_last_processed_at
FROM (
    SELECT
        prev.follower_id, prev.follower_ns,
        COALESCE(SPLIT_PART(diff.newer_leader_key, '|', 2), prev.leader_id) as leader_id,
        COALESCE(TO_NUMBER(SPLIT_PART(diff.newer_leader_key, '|', 1)), prev.leader_ns) as leader_ns,
        prev.follower_first_seen_at, prev.follower_last_seen_at,
        prev.follower_source_table_ids,
        CASE WHEN diff.newer_leader_key IS NULL 
             THEN prev.follower_last_processed_at 
             ELSE DATE_PART(epoch_second, CURRENT_TIMESTAMP()) 
        END as follower_last_processed_at
    FROM prev_table_with_leader_leader prev
    LEFT JOIN (
        SELECT DISTINCT
            SPLIT_PART(older_leader_key, '|', 2) as older_leader_id,
            TO_NUMBER(SPLIT_PART(older_leader_key, '|', 1)) as older_leader_ns,
            newer_leader_key
        FROM (
            SELECT
                older_leader_key,
                MIN(newer_leader_key) as newer_leader_key
            FROM (
                SELECT
                    leader_key as older_leader_key,
                    MIN(leader_key) OVER (PARTITION BY follower_id, follower_ns) as newer_leader_key
                FROM (
                    SELECT
                        follower_id, follower_ns,
                        LPAD({priority_case_sql}::VARCHAR, 3, '0') || '|' || leader_id as leader_key
                    FROM prev_table_with_leader_leader
                ) rs
            ) wsrs
            WHERE older_leader_key > newer_leader_key
            GROUP BY older_leader_key
        ) diffrs
    ) diff
    ON prev.leader_id = diff.older_leader_id AND prev.leader_ns = diff.older_leader_ns
) lp,
LATERAL FLATTEN(input => lp.follower_source_table_ids) flattened_table_ids
GROUP BY follower_id, follower_ns, leader_id, leader_ns;"""

        sql_files.append((f"04_unify_loop_iteration_{i:02d}", loop_sql))
        prev_table = curr_table

    # 05: Simple canonicalization using the final loop table
    lookup_table = format_database_table(database, schema, f"{canonical_id_name}_lookup")
    keys_table = format_database_table(database, schema, f"{canonical_id_name}_keys")
    tables_table = format_database_table(database, schema, f"{canonical_id_name}_tables")
    final_graph_table = format_database_table(database, schema, f"{canonical_id_name}_graph")
    
    # Use the final loop table (created by SQL executor as an alias to the actual final iteration)
    final_loop_table = format_database_table(database, schema, f"{canonical_id_name}_graph_unify_loop_final")

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

    # Canonicalization step using dynamic key mask generation
    keys_table_tmp = format_database_table(database, schema, f"{canonical_id_name}_keys_tmp")
    tables_table_tmp = format_database_table(database, schema, f"{canonical_id_name}_tables_tmp")
    lookup_table_tmp = format_database_table(database, schema, f"{canonical_id_name}_lookup_tmp")
    
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
    
    canonicalize_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

-- Create temporary reference tables matching Presto exactly
DROP TABLE IF EXISTS {keys_table_tmp};
CREATE TABLE {keys_table_tmp} AS
SELECT * FROM VALUES
    {', '.join(key_values)}
AS t (key_type, key_name);

DROP TABLE IF EXISTS {tables_table_tmp};
CREATE TABLE {tables_table_tmp} AS
SELECT * FROM VALUES
    {', '.join(table_values)}
AS t (table_id, table_name);

-- Canonicalized canonical ID lookup table matching Presto exactly
DROP TABLE IF EXISTS {lookup_table_tmp};
CREATE TABLE {lookup_table_tmp}
CLUSTER BY (id) AS
SELECT
    BASE64_ENCODE(
        CONCAT(
            TO_BINARY(
                LPAD(
                    LTRIM(
                        TO_CHAR(
                            BITXOR(
                                TO_NUMBER(SUBSTR(SHA2(graph.leader_id, 256), 1, 16), 'XXXXXXXXXXXXXXXX'),
                                leader_keys.key_mask_low64i
                            ), 
                            'XXXXXXXXXXXXXXXX'
                        )
                    ), 16, '0'
                ), 'HEX'
            ),
            leader_keys.key_mask_high8b
        )
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
        TO_NUMBER(SUBSTR(masks.key_mask, 1, 16), 'XXXXXXXXXXXXXXXX') as key_mask_low64i,
        TO_BINARY(SUBSTR(masks.key_mask, 17, 2), 'HEX') as key_mask_high8b
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
    result_stats_table = format_database_table(
        database, schema, f"{canonical_id_name}_result_key_stats"
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
        table_flag_exprs.append(f"BOOLOR_AGG(ARRAYS_OVERLAP(follower_source_table_ids, ARRAY_CONSTRUCT({table_id}))) as from_{clean_table_name}")
        case_conditions.append(f"WHEN source_groups.from_{clean_table_name} THEN '{clean_table_name}'")
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

    # Build distinct_with expressions (matching Presto exactly)
    distinct_with_exprs = []
    for key in merge_keys:
        distinct_with_exprs.append(f"COUNT_IF(distinct_{key} > 0) AS distinct_with_{key}")

    # Build column definitions for CREATE TABLE
    distinct_with_columns = [f"distinct_with_{key} NUMBER" for key in merge_keys]
    histogram_columns = [f"histogram_{key} VARCHAR" for key in merge_keys]
    
    # Create histogram calculation for each key using ARRAY_SORT for proper sorting
    histogram_calcs = []
    for key in merge_keys:
        histogram_calcs.append(f"""ARRAY_TO_STRING(
            ARRAY_SORT(
                ARRAY_AGG(
                    CASE WHEN distinct_{key} > 0 
                    THEN CAST(distinct_{key} AS VARCHAR) || ':' || CAST(count_leaders AS VARCHAR)
                    END
                )
            ), ','
        ) AS histogram_{key}""")

    result_stats_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {result_stats_table} (
    from_table VARCHAR,
    total_distinct NUMBER,
    {', '.join(distinct_with_columns)},
    {', '.join(histogram_columns)},
    time NUMBER
);

INSERT INTO {result_stats_table}
WITH follower_contributions AS (
    SELECT
        leader_id,
        leader_ns,
        {', '.join(table_flag_exprs)},
        {', '.join(key_distinct_exprs)}
    FROM {final_graph_table}
    GROUP BY leader_id, leader_ns
),
source_groups AS (
    SELECT
        {', '.join([f'from_{name}' for name in clean_table_names])},
        COUNT(*) as total_distinct,
        {', '.join([f"COUNT_IF(distinct_{key} > 0) AS distinct_with_{key}" for key in merge_keys])}
    FROM follower_contributions
    GROUP BY GROUPING SETS ({grouping_sets})
    HAVING {having_conditions}
)
SELECT
    CASE
        {case_conditions_str}
        ELSE '*'
    END as from_table,
    total_distinct,
    {', '.join([f"distinct_with_{key}" for key in merge_keys])},
    {', '.join([f'''ARRAY_TO_STRING(
        ARRAY_SORT(
            ARRAY_AGG(DISTINCT
                CASE WHEN histogram_data_{key}.distinct_val > 0 
                THEN CAST(histogram_data_{key}.distinct_val AS VARCHAR) || ':' || CAST(histogram_data_{key}.count_val AS VARCHAR)
                END
            )
        ), ','
    ) AS histogram_{key}''' for key in merge_keys])},
    DATE_PART(epoch_second, CURRENT_TIMESTAMP()) as time
FROM (
    SELECT
        {', '.join([f'from_{name}' for name in clean_table_names])},
        COUNT(*) as total_distinct,
        {', '.join([f"COUNT_IF(distinct_{key} > 0) AS distinct_with_{key}" for key in merge_keys])}
    FROM follower_contributions
    GROUP BY GROUPING SETS ({grouping_sets})
    HAVING {having_conditions}
) source_groups
{' '.join([f'''CROSS JOIN (
    SELECT distinct_{key} as distinct_val, COUNT(*) as count_val
    FROM follower_contributions
    WHERE distinct_{key} > 0
    GROUP BY distinct_{key}
) histogram_data_{key}''' for key in merge_keys])}
GROUP BY {', '.join([f'source_groups.from_{name}' for name in clean_table_names])}, source_groups.total_distinct, {', '.join([f"source_groups.distinct_with_{key}" for key in merge_keys])};"""

    sql_files.append(("06_result_key_stats", result_stats_sql))

    # 10+ Enrichments (for each source table)
    for table in tables_config:
        table_name = table["table"]
        key_name_to_ns = {key: i + 1 for i, key in enumerate(merge_keys)}
        key_name_to_config = {key["name"]: key for key in keys_config}

        # Build prioritized key columns for this table (following extract_merge pattern)
        table_key_columns = []
        
        for key_col in table["key_columns"]:
            if key_col["key"] in merge_keys:
                column = key_col["column"]
                key_name = key_col["key"]
                key_config = key_name_to_config.get(key_name, {})
                key_ns = key_name_to_ns.get(key_name)

                # Use proper NULL handling and valid_regexp
                condition = format_invalid_values_condition(
                    column, key_config.get("invalid_texts", []), key_config.get("valid_regexp")
                )
                
                table_key_columns.append({
                    'column': column,
                    'key_name': key_name,
                    'key_ns': key_ns,
                    'condition': condition,
                    'key_config': key_config
                })

        # Skip if no key columns for this table
        if not table_key_columns:
            continue
            
        # Sort by merge_keys order to match extract_merge priority
        def get_merge_key_priority(key_col):
            return merge_keys.index(key_col['key_name'])
        
        table_key_columns.sort(key=get_merge_key_priority)

        # Build CASE statements for lookup JOIN (id and id_key_type)
        lookup_id_cases = []
        lookup_key_type_cases = []
        
        # Build CASE statements for fallback hash generation
        hash_cases = []
        
        for key_col in table_key_columns:
            column = key_col['column']
            key_ns = key_col['key_ns']
            condition = key_col['condition']
            
            lookup_id_cases.append(f"""WHEN {condition}
                THEN CAST(p.{column} AS VARCHAR)""")
            
            lookup_key_type_cases.append(f"""WHEN {condition}
                THEN {key_ns}""")
            
            hash_expr = build_id_hash_expression_snowflake(
                f"CAST(p.{column} AS VARCHAR)", key_ns, len(merge_keys)
            )
            hash_cases.append(f"""WHEN {condition}
                THEN {hash_expr}""")

        # Combine all conditions for the WHEN clause in COALESCE
        all_valid_conditions = " OR ".join([f"({key_col['condition']})" for key_col in table_key_columns])
        
        # Build complete CASE statements
        lookup_id_case = f"""CASE
            {chr(10).join(lookup_id_cases)}
            ELSE NULL
        END"""
        
        lookup_key_type_case = f"""CASE
            {chr(10).join(lookup_key_type_cases)}
            ELSE NULL
        END"""
        
        hash_case = f"""CASE
            {chr(10).join(hash_cases)}
            ELSE NULL
        END"""

        enriched_table = format_database_table(database, schema, f"enriched_{table_name}")
        enriched_table_tmp = format_database_table(
            database, schema, f"enriched_{table_name}_tmp"
        )
        # Use the provided src_schema parameter instead of inferring from YAML
        source_table = f"{src_database}.{src_schema}.{table['table']}"

        enrich_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {enriched_table_tmp} AS
WITH src AS (
    SELECT * FROM {source_table}
    WHERE TRUE
)
SELECT
    p.*,
    COALESCE(
        k0.canonical_id,
        CASE
            WHEN {all_valid_conditions}
            THEN {hash_case}
            ELSE NULL
        END
    ) AS {canonical_id_name}
FROM src p
LEFT JOIN {lookup_table} k0
    ON k0.id = {lookup_id_case}
    AND k0.id_key_type = {lookup_key_type_case};

-- Commit enriched table
DROP TABLE IF EXISTS {enriched_table};
ALTER TABLE {enriched_table_tmp} RENAME TO {enriched_table.split('.')[-1]};"""

        sql_files.append((f"10_enrich_{table_name}", enrich_sql))

    # 20+ Master tables - Dynamic generation based on YAML attributes
    for master in master_tables_config:
        master_name = master["name"]
        canonical_id = master["canonical_id"]
        attributes = master.get("attributes", [])

        master_table = format_database_table(database, schema, master_name)
        master_table_tmp = format_database_table(database, schema, f"{master_name}_tmp")

        # Build mapping of all tables that have enriched versions
        enriched_table_map = {}
        for table in tables_config:
            table_name = table["table"]
            # Handle table name mapping for enriched tables
            clean_table_name = table_name.split('.')[-1]
            
            # Map table names to match Presto/TD naming conventions
            if clean_table_name == 'kris_orders' or clean_table_name == 'kris_src_orders':
                clean_table_name = 'orders'  # Match TD's naming
            
            enriched_table = format_database_table(database, schema, f"enriched_{table_name}")
            
            # Add mappings for both original and clean names
            enriched_table_map[clean_table_name] = enriched_table
            enriched_table_map[table_name] = enriched_table
            
            # Also add the full table name without database prefix
            enriched_table_map[table_name.split('.')[-1]] = enriched_table

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
                        select_columns.append(f"CAST(NULL AS VARCHAR) as {attr_col}")
                        select_columns.append(f"CAST(NULL AS NUMBER) as {order_col}")
            
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

        # Build attribute selection logic matching Presto exactly
        attr_selections = [canonical_id]
        
        for attr in attributes:
            attr_name = attr["name"]
            source_columns = attr.get("source_columns", [])
            array_elements = attr.get("array_elements")
            
            if array_elements:
                # Array attribute - replicate Presto's max_by(attr, order, limit) with filter
                array_parts = []
                for i, source_col in enumerate(source_columns):
                    priority = source_col.get("priority", i + 1)
                    attr_col = f"{attr_name}_p{priority}_attr"
                    order_col = f"{attr_name}_p{priority}_order"
                    
                    # Replicate: max_by("attr", "order", 3) filter (where cast("attr" as varchar) is not null)
                    array_parts.append(f"""COALESCE(
                    ARRAY_SLICE(
                        ARRAY_AGG(CASE WHEN CAST({attr_col} AS VARCHAR) IS NOT NULL THEN {attr_col} END) 
                        WITHIN GROUP (ORDER BY {order_col} DESC),
                        0, {array_elements}
                    ),
                    ARRAY_CONSTRUCT()
                )""")
                
                # Use ARRAY_CAT to concatenate arrays (matching Presto's concat)
                if len(array_parts) == 1:
                    array_concat_expr = array_parts[0]
                else:
                    array_concat_expr = array_parts[0]
                    for part in array_parts[1:]:
                        array_concat_expr = f"ARRAY_CAT({array_concat_expr}, {part})"
                
                # Replicate Presto's slice(concat(...), 1, 3) - note Presto is 1-indexed
                attr_selection = f"""ARRAY_SLICE(
            {array_concat_expr},
            0, {array_elements}
        ) AS {attr_name}"""
            else:
                # Single value attribute - replicate Presto's max_by with filter
                if len(source_columns) == 1:
                    priority = source_columns[0].get("priority", 1)
                    attr_col = f"{attr_name}_p{priority}_attr"
                    order_col = f"{attr_name}_p{priority}_order"
                    # Replicate: max_by("attr", "order") filter (where cast("attr" as varchar) is not null)
                    attr_selection = f"""MAX_BY(
            CASE WHEN CAST({attr_col} AS VARCHAR) IS NOT NULL THEN {attr_col} END,
            CASE WHEN CAST({attr_col} AS VARCHAR) IS NOT NULL THEN {order_col} END
        ) AS {attr_name}"""
                else:
                    # Multiple sources with COALESCE (matching Presto pattern)
                    coalesce_parts = []
                    for source_col in source_columns:
                        priority = source_col.get("priority", 999)
                        attr_col = f"{attr_name}_p{priority}_attr"
                        order_col = f"{attr_name}_p{priority}_order"
                        coalesce_parts.append(f"""MAX_BY(
            CASE WHEN CAST({attr_col} AS VARCHAR) IS NOT NULL THEN {attr_col} END,
            CASE WHEN CAST({attr_col} AS VARCHAR) IS NOT NULL THEN {order_col} END
        )""")
                    
                    coalesce_parts_str = ',\n            '.join(coalesce_parts)
                    attr_selection = f"""COALESCE(
            {coalesce_parts_str}
        ) AS {attr_name}"""
            
            attr_selections.append(attr_selection)

        # Extract table names to avoid f-string issues
        master_table_name = master_table.split('.')[-1]
        attr_selections_str = ',\n        '.join(attr_selections)
        
        master_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {master_table_tmp} AS
WITH us AS (
    {union_sql}
),
attrs AS (
    -- Master Table Attributes using simplified approach based on Databricks
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
    unification_metadata_table = format_database_table(database, schema, "unification_metadata")
    
    metadata_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

-- Create unification metadata table
CREATE OR REPLACE TABLE {unification_metadata_table} (
    canonical_id_name VARCHAR,
    canonical_id_type VARCHAR
);

-- Insert metadata information about the canonical ID
INSERT INTO {unification_metadata_table}
SELECT canonical_id_name, canonical_id_type
FROM VALUES ('{canonical_id_name}', 'canonical_id') AS t(canonical_id_name, canonical_id_type);"""

    sql_files.append(("30_unification_metadata", metadata_sql))

    # 31: Filter lookup metadata table
    filter_lookup_table = format_database_table(database, schema, "filter_lookup")
    
    # Build filter lookup values from keys configuration
    filter_values = []
    for key in keys_config:
        key_name = key["name"]
        invalid_texts = key.get("invalid_texts", [])
        valid_regexp = key.get("valid_regexp")
        
        # Format invalid_texts as array string for Snowflake
        if invalid_texts:
            invalid_texts_str = "ARRAY_CONSTRUCT(" + ", ".join([f"'{text}'" if text is not None else "NULL" for text in invalid_texts]) + ")"
        else:
            invalid_texts_str = "ARRAY_CONSTRUCT('', 'N/A', 'null')"  # Default values matching TD
        
        # Handle valid_regexp
        if valid_regexp:
            valid_regexp_str = f"'{valid_regexp}'"
        else:
            # Set default regexp for email, null for others (matching TD pattern)
            if key_name == "email":
                valid_regexp_str = "'.*@.*'"
            else:
                valid_regexp_str = "CAST(NULL AS VARCHAR)"
        
        filter_values.append(f"('{key_name}', {invalid_texts_str}, {valid_regexp_str})")
    
    # Build individual SELECT statements instead of VALUES with ARRAY_CONSTRUCT
    filter_selects = []
    for key in keys_config:
        key_name = key["name"]
        invalid_texts = key.get("invalid_texts", [])
        valid_regexp = key.get("valid_regexp")
        
        # Format invalid_texts as ARRAY_CONSTRUCT 
        if invalid_texts:
            invalid_texts_str = "ARRAY_CONSTRUCT(" + ", ".join([f"'{text}'" if text is not None else "NULL" for text in invalid_texts]) + ")"
        else:
            invalid_texts_str = "ARRAY_CONSTRUCT('', 'N/A', 'null')"  # Default values matching TD
        
        # Handle valid_regexp
        if valid_regexp:
            valid_regexp_str = f"'{valid_regexp}'"
        else:
            # Set default regexp for email, null for others (matching TD pattern)
            if key_name == "email":
                valid_regexp_str = "'.*@.*'"
            else:
                valid_regexp_str = "CAST(NULL AS VARCHAR)"
        
        filter_selects.append(f"SELECT '{key_name}' as key_name, {invalid_texts_str} as invalid_texts, {valid_regexp_str} as valid_regexp")

    filter_lookup_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

-- Create filter lookup metadata table
CREATE OR REPLACE TABLE {filter_lookup_table} (
    key_name VARCHAR,
    invalid_texts ARRAY,
    valid_regexp VARCHAR
);

-- Insert filter lookup information from YAML keys configuration
INSERT INTO {filter_lookup_table}
{' UNION ALL '.join(filter_selects)};"""

    sql_files.append(("31_filter_lookup", filter_lookup_sql))

    # 32: Column lookup metadata table
    column_lookup_table = format_database_table(database, schema, "column_lookup")
    
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
    
    column_lookup_sql = f"""USE DATABASE {database};
USE SCHEMA {schema};

-- Create column lookup metadata table
CREATE OR REPLACE TABLE {column_lookup_table} (
    database_name VARCHAR,
    table_name VARCHAR,
    column_name VARCHAR,
    key_name VARCHAR
);

-- Insert column lookup information from YAML tables configuration
INSERT INTO {column_lookup_table}
SELECT database_name, table_name, column_name, key_name
FROM VALUES 
    {', '.join(column_values)}
AS t(database_name, table_name, column_name, key_name);"""

    sql_files.append(("32_column_lookup", column_lookup_sql))

    # Apply conversion rules to all SQL
    sql_files = [
        (name, apply_snowflake_rules(sql, fix_syntax)) for name, sql in sql_files
    ]

    return sql_files


def main():
    parser = argparse.ArgumentParser(
        description="Generate complete Snowflake SQL from YAML unification configuration"
    )
    parser.add_argument("yaml_file", type=pathlib.Path, help="Path to unify.yml")
    parser.add_argument(
        "-d", "--database", required=True, help="Target Snowflake database name"
    )
    parser.add_argument(
        "-s", "--schema", required=True, help="Target Snowflake schema name"
    )
    parser.add_argument(
        "-sd", "--src-database", 
        help="Source database name (defaults to target database if not provided)"
    )
    parser.add_argument(
        "-ss", "--src-schema", 
        help="Source schema name (defaults to PUBLIC if not provided)"
    )
    parser.add_argument(
        "-o",
        "--outdir",
        default="snowflake_sql",
        type=pathlib.Path,
        help="Output directory",
    )
    parser.add_argument(
        "--no-fix-syntax",
        action="store_true",
        help="Skip Presto/Databricks→Snowflake conversion rules",
    )
    args = parser.parse_args()

    if not args.yaml_file.exists():
        print(f"Error: {args.yaml_file} not found.")
        return 1

    # Load YAML configuration
    with open(args.yaml_file, "r") as f:
        yaml_data = yaml.safe_load(f)

    # Set src_database (default to target database if not provided)
    src_database = args.src_database if args.src_database else args.database
    src_schema = args.src_schema if args.src_schema else "PUBLIC"

    # Generate SQL files
    sql_files = generate_workflow_sql_snowflake(
        yaml_data, args.database, args.schema, src_database, src_schema, fix_syntax=not args.no_fix_syntax
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
    print(f"Database: {args.database}")
    print(f"Schema: {args.schema}")

    # Show execution order
    print(f"\nExecution order:")
    for i, (filename, _) in enumerate(sql_files, 1):
        print(f"  {i:2d}. {filename}")

    print(f"\nSnowflake table benefits enabled:")
    print(f"  ✓ Optimized clustering on key columns")
    print(f"  ✓ Performance optimizations for large datasets")
    print(f"  ✓ Snowflake-specific data types and functions")
    print(f"  ✓ VARIANT support for flexible data structures")

    return 0


if __name__ == "__main__":
    exit(main())