#!/usr/bin/env python3
"""
snowflake_sql_executor.py
────────────────────────────────────────────────────────────────────
Execute generated Snowflake SQL files in the correct sequence.

Features:
 - Executes SQL files in numerical order (01_, 02_, 03_, etc.)
 - Handles unify loop logic with proper stop conditions
 - Provides detailed logging and error handling
 - Supports dry-run mode for validation
 - Database connection management

Usage:
 $ python snowflake_sql_executor.py snowflake_sql/unify/ --account myaccount --user myuser --warehouse my_datawarehouse --database my_database

Dependencies:
 - snowflake-connector-python
 - rich

Install:
 pip install snowflake-connector-python rich

    parser.add_argument("--account", required=True, help="Snowflake account name")
    parser.add_argument("--user", required=True, help="Snowflake username")
    parser.add_argument("--warehouse", default="COMPUTE_WH", help="Snowflake warehouse")
    parser.add_argument("--database", required=True, help="Snowflake database")
    parser.add_argument("--schema", default="PUBLIC", help="Snowflake schema")

"""

import argparse
import getpass
import pathlib
import re
import sys
import time
import os
import yaml
from typing import List, Tuple, Optional

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    print("Warning: snowflake-connector-python not available. Dry-run mode only.")

from rich import print
from rich.console import Console
from rich.table import Table
from dotenv import load_dotenv

# This line loads variables from the .env file into the environment
load_dotenv()

console = Console()


def load_unify_config(config_path: pathlib.Path) -> dict:
    """Load unify.yml configuration"""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"[red]Error loading config {config_path}: {e}[/red]")
        return {}


class SnowflakeExecutor:
    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str = "PUBLIC",
        config: dict = None,
    ):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None
        self.cursor = None
        self.config = config or {}
        
        # Extract table name prefix from config
        canonical_ids = self.config.get('canonical_ids', [])
        self.table_prefix = canonical_ids[0].get('name', 'td_id') if canonical_ids else 'td_id'

    def connect(self):
        """Establish connection to Snowflake"""
        if not SNOWFLAKE_AVAILABLE:
            print(f"[red]✗[/red] Snowflake connector not available. Install with: pip install snowflake-connector-python")
            return False
            
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )
            self.cursor = self.connection.cursor()
            
            # Set database and schema context
            self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")
            
            print(f"[green]✓[/green] Connected to Snowflake: {self.account}")
            print(f"[cyan]•[/cyan] Using database: {self.database}, schema: {self.schema}")
            return True
        except Exception as e:
            print(f"[red]✗[/red] Failed to connect to Snowflake: {e}")
            return False

    def disconnect(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print(f"[yellow]•[/yellow] Disconnected from Snowflake")

    def execute_sql(
        self, sql: str, description: str = ""
    ) -> Tuple[bool, Optional[int], str]:
        """
        Execute SQL statement
        Returns: (success, row_count, message)
        """
        try:
            # Split SQL into individual statements
            statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]

            total_rows = 0
            for stmt in statements:
                # Skip empty statements
                if not stmt:
                    continue

                # Handle different statement types
                if stmt.upper().startswith(("USE DATABASE", "USE SCHEMA", "USE WAREHOUSE")):
                    # Execute context-setting statements
                    self.cursor.execute(stmt)
                elif stmt.upper().startswith("SET "):
                    # Execute configuration statements
                    self.cursor.execute(stmt)
                else:
                    # Execute main statement
                    result = self.cursor.execute(stmt)

                    # Try to get row count (may not be available for all operations)
                    try:
                        if hasattr(result, "rowcount") and result.rowcount >= 0:
                            total_rows += result.rowcount
                        else:
                            # For some operations, try to get affected rows differently
                            rows = self.cursor.fetchall()
                            if rows:
                                total_rows += len(rows)
                    except Exception:
                        # Row count not available for this operation type
                        if stmt.upper().strip().startswith("INSERT"):
                            try:
                                # Try to extract table name and count rows if possible
                                pass
                            except:
                                pass

            return True, total_rows if total_rows > 0 else None, "Executed successfully"

        except Exception as e:
            error_msg = str(e)

            # Handle common Snowflake errors with helpful messages
            if "WAREHOUSE" in error_msg:
                error_msg = f"Warehouse error: {error_msg}"
            elif "DATABASE" in error_msg and "does not exist" in error_msg:
                error_msg = f"Database '{self.database}' not found: {error_msg}"
            elif "SCHEMA" in error_msg and "does not exist" in error_msg:
                error_msg = f"Schema '{self.schema}' not found: {error_msg}"
            elif "TABLE" in error_msg and "does not exist" in error_msg:
                error_msg = f"Table not found: {error_msg}"

            return False, None, f"Error: {error_msg}"

    def check_unify_loop_convergence(
        self, prev_table: str, curr_table: str
    ) -> Tuple[int, bool]:
        """
        Check if unify loop has converged
        Returns: (updated_count, should_continue)
        """
        try:
            check_sql = f"""
            SELECT COUNT(*) as updated_count FROM (
                SELECT leader_ns, leader_id, follower_ns, follower_id FROM {curr_table}
                EXCEPT
                SELECT leader_ns, leader_id, follower_ns, follower_id FROM {prev_table}
            ) diff
            """

            result = self.cursor.execute(check_sql)
            row = result.fetchone()
            updated_count = row[0] if row else 0

            should_continue = updated_count > 0
            return updated_count, should_continue

        except Exception as e:
            print(f"[red]✗[/red] Error checking convergence: {e}")
            return 0, False

    def get_table_info(self, table_name: str) -> Optional[dict]:
        """
        Get basic information about a Snowflake table
        """
        try:
            full_table_name = f"{self.database}.{self.schema}.{table_name}"

            # Get row count
            count_result = self.cursor.execute(
                f"SELECT COUNT(*) FROM {full_table_name}"
            )
            row_count = count_result.fetchone()[0] if count_result else 0

            # Get table info
            info_result = self.cursor.execute(f"DESCRIBE TABLE {full_table_name}")
            columns = info_result.fetchall() if info_result else []

            return {
                "row_count": row_count,
                "column_count": len(columns),
                "table_type": "SNOWFLAKE_TABLE",
            }

        except Exception as e:
            print(f"[yellow]⚠[/yellow] Could not get table info for {table_name}: {e}")
            return None


def generate_iteration_sql(executor: SnowflakeExecutor, iteration: int) -> str:
    """Generate SQL for a unify loop iteration dynamically"""
    prev_iteration = iteration - 1
    table_prefix = executor.table_prefix
    database = executor.database
    schema = executor.schema
    
    return f"""
USE DATABASE {database};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE {database}.{schema}.{table_prefix}_graph_unify_loop_{iteration} (
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

INSERT INTO {database}.{schema}.{table_prefix}_graph_unify_loop_{iteration}
WITH prev_table_with_leader_leader AS (
    SELECT
        follower_id, follower_ns, leader_id, leader_ns,
        follower_first_seen_at, follower_last_seen_at,
        follower_source_table_ids, follower_last_processed_at
    FROM {database}.{schema}.{table_prefix}_graph_unify_loop_{prev_iteration}
    
    UNION ALL
    
    -- leader -> leader relationship (corrected mapping)
    SELECT
        follower_id, follower_ns, leader_id, leader_ns,
        follower_first_seen_at, follower_last_seen_at,
        follower_source_table_ids, follower_last_processed_at
    FROM (
        SELECT DISTINCT leader_id, leader_ns FROM {database}.{schema}.{table_prefix}_graph_unify_loop_{prev_iteration}
    ) prev_leaders
    INNER JOIN (
        SELECT
            follower_id, follower_ns,
            follower_first_seen_at, follower_last_seen_at,
            follower_source_table_ids, follower_last_processed_at
        FROM {database}.{schema}.{table_prefix}_graph_unify_loop_{prev_iteration}
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
                        LPAD(CASE leader_ns WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 3 ELSE leader_ns END::VARCHAR, 3, '0') || '|' || leader_id as leader_key
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
GROUP BY follower_id, follower_ns, leader_id, leader_ns;
"""


def get_sql_files(sql_dir: pathlib.Path) -> List[Tuple[str, pathlib.Path]]:
    """Get SQL files in execution order"""
    files = []

    # Find all SQL files
    for sql_file in sql_dir.glob("*.sql"):
        # Extract order prefix (01_, 02_, etc.)
        match = re.match(r"^(\d+)_(.+)\.sql$", sql_file.name)
        if match:
            order = int(match.group(1))
            name = match.group(2)
            files.append((f"{order:02d}_{name}", sql_file))
        else:
            # Files without order prefix go last
            files.append((f"99_{sql_file.stem}", sql_file))

    # Sort by order
    files.sort(key=lambda x: x[0])
    return files


def execute_unify_loop(executor: SnowflakeExecutor, sql_dir: pathlib.Path, max_iterations: int = 30) -> int:
    """Execute unify loop with convergence checking - continues beyond available files if needed"""
    loop_files = sorted(sql_dir.glob("04_*iter*.sql"))
    print(
        f"\n[bold cyan]Executing Unify Loop "
        f"({len(loop_files)} files available, max {max_iterations} iterations)[/bold cyan]"
    )

    if not loop_files:
        print("[yellow]No loop iteration files found[/yellow]")
        return 0

    prev_table = f"{executor.table_prefix}_graph_unify_loop_0"
    executed_count = 0
    final_iteration = 0
    
    # Continue iterating until convergence or max_iterations reached
    iteration = 1
    while iteration <= max_iterations:
        print(f"\n[yellow]--- Iteration {iteration} ---[/yellow]")
        
        # Use file if available, otherwise generate SQL dynamically
        if iteration <= len(loop_files):
            file_path = loop_files[iteration - 1]
            sql = file_path.read_text()
            source_desc = f"file: {file_path.name}"
        else:
            sql = generate_iteration_sql(executor, iteration)
            source_desc = f"dynamically generated iteration {iteration}"
        
        print(f"[cyan]•[/cyan] Using {source_desc}")

        # Execute the iteration
        ok, rows, msg = executor.execute_sql(sql, f"iteration_{iteration}")
        if not ok:
            print(f"[red]✗[/red] {msg}")
            return executed_count

        executed_count += 1
        final_iteration = iteration
        print(f"[green]✓[/green] Iteration {iteration} completed")

        if rows:
            print(f"[cyan]•[/cyan] Rows processed: {rows}")

        # Check convergence after EVERY iteration (including first)
        curr_table = f"{executor.table_prefix}_graph_unify_loop_{iteration}"
        updated, cont = executor.check_unify_loop_convergence(prev_table, curr_table)
        print(f"[cyan]•[/cyan] Updated records: {updated}")

        if not cont:  # convergence reached (updated_count = 0)
            print(f"[green]✓[/green] Loop converged after {iteration} iterations")
            break

        prev_table = curr_table
        iteration += 1
        time.sleep(2)  # Brief pause between iterations
    
    if iteration > max_iterations:
        print(f"[yellow]⚠[/yellow] Reached maximum iterations ({max_iterations}) without convergence")

    # Create alias table pointing to the final iteration for subsequent steps
    if final_iteration > 0:
        final_table_name = f"{executor.table_prefix}_graph_unify_loop_{final_iteration}"
        alias_table_name = f"{executor.table_prefix}_graph_unify_loop_final"
        alias_sql = f"""
        CREATE OR REPLACE TABLE {executor.database}.{executor.schema}.{alias_table_name} 
        AS SELECT * FROM {executor.database}.{executor.schema}.{final_table_name}
        """
        
        print(f"[cyan]•[/cyan] Creating alias table for final iteration: {final_table_name}")
        ok, rows, msg = executor.execute_sql(alias_sql, "Create final iteration alias")
        if ok:
            print(f"[green]✓[/green] Alias table '{alias_table_name}' created")
        else:
            print(f"[yellow]⚠[/yellow] Failed to create alias table: {msg}")

    return executed_count


def main():
    parser = argparse.ArgumentParser(
        description="Execute Snowflake SQL files in sequence"
    )
    parser.add_argument(
        "sql_dir", type=pathlib.Path, help="Directory containing SQL files"
    )
    parser.add_argument("--account", required=True, help="Snowflake account name")
    parser.add_argument("--user", required=True, help="Snowflake username")
    parser.add_argument("--warehouse", default="COMPUTE_WH", help="Snowflake warehouse")
    parser.add_argument("--database", required=True, help="Snowflake database")
    parser.add_argument("--schema", default="PUBLIC", help="Snowflake schema")
    parser.add_argument(
        "--password",
        help="Snowflake password (or set SNOWFLAKE_PASSWORD env var)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show execution plan without running"
    )
    parser.add_argument(
        "--skip-loop", action="store_true", help="Skip unify loop iterations"
    )
    parser.add_argument(
        "--config",
        type=pathlib.Path,
        help="Path to unify.yml config file (optional, will try to find it automatically)",
    )

    args = parser.parse_args()

    if not args.sql_dir.exists() or not args.sql_dir.is_dir():
        print(f"[red]Error:[/red] SQL directory not found: {args.sql_dir}")
        return 1

    # Load config file
    config_path = args.config
    if not config_path:
        # Try to find unify.yml in the same directory as sql_dir or parent directories
        potential_paths = [
            args.sql_dir.parent / "unify.yml",
            args.sql_dir / "unify.yml",
            pathlib.Path("unify.yml")
        ]
        for path in potential_paths:
            if path.exists():
                config_path = path
                break
    
    config = {}
    if config_path and config_path.exists():
        config = load_unify_config(config_path)
        print(f"[cyan]•[/cyan] Loaded config: {config_path}")
    else:
        print(f"[yellow]⚠[/yellow] No config file found, using defaults")

    # Get password from argument or environment variable
    password = args.password
    if not password:
        password = os.getenv("SNOWFLAKE_PASSWORD")
        if not password:
            password = getpass.getpass(f"Password for {args.user}@{args.account}: ")

    # Get SQL files in order
    sql_files = get_sql_files(args.sql_dir)

    if not sql_files:
        print(f"[red]Error:[/red] No SQL files found in {args.sql_dir}")
        return 1

    # Show execution plan
    table = Table(title="Execution Plan")
    table.add_column("Order", style="cyan")
    table.add_column("File", style="yellow")
    table.add_column("Type", style="green")

    for order_name, file_path in sql_files:
        file_type = "Setup"
        if "loop" in order_name:
            file_type = "Loop Iteration"
        elif "enrich" in order_name:
            file_type = "Enrichment"
        elif "master" in order_name:
            file_type = "Master Table"
        elif "canonicalize" in order_name:
            file_type = "Canonicalization"
        elif "stats" in order_name:
            file_type = "Statistics"
        elif "meta" in order_name or "lookup" in order_name:
            file_type = "Metadata"

        table.add_row(order_name, file_path.name, file_type)

    console.print(table)

    if args.dry_run:
        print(f"\n[yellow]Dry run complete. Found {len(sql_files)} files.[/yellow]")
        print(f"[cyan]Target:[/cyan] {args.database}.{args.schema}")
        return 0

    # Execute SQL files
    executor = SnowflakeExecutor(
        account=args.account,
        user=args.user,
        password=password,
        warehouse=args.warehouse,
        database=args.database,
        schema=args.schema,
        config=config,
    )

    if not executor.connect():
        return 1

    try:
        success_count = 0

        print(f"\n[bold]Starting Snowflake SQL Execution[/bold]")
        print(f"[cyan]•[/cyan] Database: {args.database}")
        print(f"[cyan]•[/cyan] Schema: {args.schema}")

        # Execute unify loop before canonicalize step (after file 04_ files are skipped)
        unify_loop_executed = False
        user_chose_to_stop = False
        
        for order_name, file_path in sql_files:
            # Skip loop iterations if we're handling them separately
            if "loop_iteration" in order_name and not args.skip_loop:
                continue
            
            # Execute unify loop before canonicalize step  
            if "canonicalize" in order_name and not args.skip_loop and not unify_loop_executed:
                print(f"\n[bold magenta]Executing Unify Loop Before Canonicalization[/bold magenta]")
                executed_loops = execute_unify_loop(executor, args.sql_dir)
                success_count += executed_loops
                unify_loop_executed = True

            print(f"\n[bold]Executing: {file_path.name}[/bold]")

            sql_content = file_path.read_text(encoding="utf-8")
            success, rows, message = executor.execute_sql(sql_content, file_path.name)

            if success:
                print(f"[green]✓[/green] {file_path.name}: {message}")
                if rows is not None and rows > 0:
                    print(f"[cyan]•[/cyan] Rows affected: {rows}")
                success_count += 1
            else:
                print(f"[red]✗[/red] {file_path.name}: {message}")

                # Ask user if they want to continue
                response = input("Continue with remaining files? (y/n): ").lower()
                if response != "y":
                    user_chose_to_stop = True
                    print(f"[yellow]•[/yellow] Execution stopped by user choice")
                    break

        # Execute unify loop separately (if not skipped and not already executed and user didn't choose to stop)
        if not args.skip_loop and not unify_loop_executed and not user_chose_to_stop:
            print(f"\n[bold magenta]Executing Unify Loop (Fallback)[/bold magenta]")
            executed_loops = execute_unify_loop(executor, args.sql_dir)
            success_count += executed_loops

        print(f"\n[bold green]Execution Complete[/bold green]")
        print(f"[cyan]•[/cyan] Files processed: {success_count}/{len(sql_files)}")

        # Show some final stats if possible
        try:
            lookup_table_name = f"{executor.table_prefix}_lookup"
            lookup_info = executor.get_table_info(lookup_table_name)
            if lookup_info:
                print(
                    f"[cyan]•[/cyan] Final {lookup_table_name} rows: {lookup_info['row_count']:,}"
                )
        except:
            pass

    finally:
        executor.disconnect()

    return 0


if __name__ == "__main__":
    exit(main())
