---
name: hybrid-unif-merge-stats-creator
description: Generate professional HTML/PDF merge statistics report from ID unification results for Snowflake or Databricks with expert analysis and visualizations
---

# ID Unification Merge Statistics Report Generator

## Overview

I'll generate a **comprehensive, professional HTML report** analyzing your ID unification merge statistics with:

- 📊 **Executive Summary** with key performance indicators
- 📈 **Identity Resolution Performance** analysis and deduplication rates
- 🎯 **Merge Distribution** patterns and complexity analysis
- 👥 **Top Merged Profiles** highlighting complex identity resolutions
- ✅ **Data Quality Metrics** with coverage percentages
- 🚀 **Convergence Analysis** showing iteration performance
- 💡 **Expert Recommendations** for optimization and next steps

**Platform Support:**
- ✅ Snowflake (using Snowflake MCP tools)
- ✅ Databricks (using Databricks MCP tools)

**Output Format:**
- Beautiful HTML report with charts, tables, and visualizations
- PDF-ready (print to PDF from browser)
- Consistent formatting every time
- Platform-agnostic design

---

## What You Need to Provide

### 1. Platform Selection
- **Snowflake**: For Snowflake-based ID unification
- **Databricks**: For Databricks-based ID unification

### 2. Database/Catalog Configuration

**For Snowflake:**
- **Database Name**: Where your unification tables are stored (e.g., `INDRESH_TEST`, `CUSTOMER_CDP`)
- **Schema Name**: Schema containing tables (e.g., `PUBLIC`, `ID_UNIFICATION`)

**For Databricks:**
- **Catalog Name**: Unity Catalog name (e.g., `customer_data`, `cdp_prod`)
- **Schema Name**: Schema containing tables (e.g., `id_unification`, `unified_profiles`)

### 3. Canonical ID Configuration
- **Canonical ID Name**: Name used for your unified ID (e.g., `td_id`, `unified_customer_id`, `master_id`)
  - This is used to find the correct tables: `{canonical_id}_lookup`, `{canonical_id}_master_table`, etc.

### 4. Output Configuration (Optional)
- **Output File Path**: Where to save the HTML report (default: `id_unification_report.html`)
- **Report Title**: Custom title for the report (default: "ID Unification Merge Statistics Report")

---

## What I'll Do

### Step 1: Platform Detection and Validation

**Snowflake:**
```
1. Verify Snowflake MCP tools are available
2. Test connection to specified database.schema
3. Validate canonical ID tables exist:
   - {database}.{schema}.{canonical_id}_lookup
   - {database}.{schema}.{canonical_id}_master_table
   - {database}.{schema}.{canonical_id}_source_key_stats
   - {database}.{schema}.{canonical_id}_result_key_stats
4. Confirm access permissions
```

**Databricks:**
```
1. Verify Databricks MCP tools are available (or use Snowflake fallback)
2. Test connection to specified catalog.schema
3. Validate canonical ID tables exist
4. Confirm access permissions
```

### Step 2: Data Collection with Expert Analysis

I'll execute **16 specialized queries** to collect comprehensive statistics:

**Core Statistics Queries:**

1. **Source Key Statistics**
   - Pre-unification identity counts
   - Distinct values per key type (customer_id, email, phone, etc.)
   - Per-table breakdowns

2. **Result Key Statistics**
   - Post-unification canonical ID counts
   - Distribution histograms
   - Coverage per key type

3. **Canonical ID Metrics**
   - Total identities processed
   - Unique canonical IDs created
   - Merge ratio calculation

4. **Top Merged Profiles**
   - Top 10 most complex merges
   - Identity count per canonical ID
   - Merge complexity scoring

5. **Merge Distribution Analysis**
   - Categorization (2, 3-5, 6-10, 10+ identities)
   - Percentage distribution
   - Pattern analysis

6. **Key Type Distribution**
   - Identity breakdown by type
   - Namespace analysis
   - Cross-key coverage

7. **Master Table Quality Metrics**
   - Attribute coverage percentages
   - Data completeness analysis
   - Sample record extraction

8. **Configuration Metadata**
   - Unification settings
   - Column mappings
   - Validation rules

**Platform-Specific SQL Adaptation:**

For **Snowflake**:
```sql
SELECT COUNT(*) as total_identities,
       COUNT(DISTINCT canonical_id) as unique_canonical_ids
FROM {database}.{schema}.{canonical_id}_lookup;
```

For **Databricks**:
```sql
SELECT COUNT(*) as total_identities,
       COUNT(DISTINCT canonical_id) as unique_canonical_ids
FROM {catalog}.{schema}.{canonical_id}_lookup;
```

### Step 3: Statistical Analysis and Calculations

I'll perform expert-level calculations:

**Deduplication Rates:**
```
For each key type:
- Source distinct count (pre-unification)
- Final canonical IDs (post-unification)
- Deduplication % = (source - final) / source * 100
```

**Merge Ratios:**
```
- Average identities per customer = total_identities / unique_canonical_ids
- Distribution across categories
- Outlier detection (10+ merges)
```

**Convergence Analysis:**
```
- Parse from execution logs if available
- Calculate from iteration metadata tables
- Estimate convergence quality
```

**Data Quality Scores:**
```
- Coverage % for each attribute
- Completeness assessment
- Quality grading (Excellent, Good, Needs Improvement)
```

### Step 4: HTML Report Generation

I'll generate a **pixel-perfect HTML report** with:

**Design Features:**
- ✨ Modern gradient design (purple theme)
- 📊 Interactive visualizations (progress bars, horizontal bar charts)
- 🎨 Color-coded badges and status indicators
- 📱 Responsive layout (works on all devices)
- 🖨️ Print-optimized CSS for PDF export

**Report Structure:**

```html
<!DOCTYPE html>
<html>
  <head>
    - Professional CSS styling
    - Chart/visualization styles
    - Print media queries
  </head>
  <body>
    <header>
      - Report title
      - Executive tagline
    </header>

    <metadata-bar>
      - Database/Catalog info
      - Canonical ID name
      - Generation timestamp
      - Platform indicator
    </metadata-bar>

    <section: Executive Summary>
      - 4 KPI metric cards
      - Key findings insight box
    </section>

    <section: Identity Resolution Performance>
      - Source vs result comparison table
      - Deduplication rate analysis
      - Horizontal bar charts
      - Expert insights
    </section>

    <section: Merge Distribution Analysis>
      - Category breakdown table
      - Distribution visualizations
      - Pattern analysis insights
    </section>

    <section: Top Merged Profiles>
      - Top 10 ranked table
      - Complexity badges
      - Investigation recommendations
    </section>

    <section: Source Table Configuration>
      - Column mapping table
      - Source contributions
      - Multi-key strategy analysis
    </section>

    <section: Master Table Data Quality>
      - 6 coverage cards with progress bars
      - Sample records table
      - Quality assessment
    </section>

    <section: Convergence Performance>
      - Iteration breakdown table
      - Convergence progression chart
      - Efficiency analysis
    </section>

    <section: Expert Recommendations>
      - 4 recommendation cards
      - Strategic next steps
      - Downstream activation ideas
    </section>

    <section: Summary Statistics>
      - Comprehensive metrics table
      - All key numbers documented
    </section>

    <footer>
      - Generation metadata
      - Platform information
      - Report description
    </footer>
  </body>
</html>
```

### Step 5: Quality Validation and Output

**Pre-Output Validation:**
```
1. Verify all sections have data
2. Check calculations are correct
3. Validate percentages sum properly
4. Ensure no missing values
5. Confirm HTML is well-formed
```

**File Output:**
```
1. Write HTML to specified path
2. Create backup if file exists
3. Set proper file permissions
4. Verify file was written successfully
```

**Report Summary:**
```
✓ Report generated: {file_path}
✓ File size: {size} KB
✓ Sections included: 9
✓ Statistics queries: 16
✓ Data quality score: {score}%
✓ Ready for: Browser viewing, PDF export, sharing
```

---

## Example Workflow

### Snowflake Example

**User Input:**
```
Platform: Snowflake
Database: INDRESH_TEST
Schema: PUBLIC
Canonical ID: td_id
Output: snowflake_merge_report.html
```

**Process:**
```
✓ Connected to Snowflake via MCP
✓ Database: INDRESH_TEST.PUBLIC validated
✓ Tables found:
  - td_id_lookup (19,512 records)
  - td_id_master_table (4,940 records)
  - td_id_source_key_stats (4 records)
  - td_id_result_key_stats (4 records)

Executing queries:
  ✓ Query 1: Source statistics retrieved
  ✓ Query 2: Result statistics retrieved
  ✓ Query 3: Canonical ID counts (19,512 → 4,940)
  ✓ Query 4: Top 10 merged profiles identified
  ✓ Query 5: Merge distribution calculated
  ✓ Query 6: Key type distribution analyzed
  ✓ Query 7: Master table coverage (100% email, 99.39% phone)
  ✓ Query 8: Sample records extracted
  ✓ Query 9-11: Metadata retrieved

Calculating metrics:
  ✓ Merge ratio: 3.95:1
  ✓ Fragmentation reduction: 74.7%
  ✓ Deduplication rates:
    - customer_id: 23.9%
    - email: 32.0%
    - phone: 14.8%
  ✓ Data quality score: 99.7%

Generating HTML report:
  ✓ Executive summary section
  ✓ Performance analysis section
  ✓ Merge distribution section
  ✓ Top profiles section
  ✓ Source configuration section
  ✓ Data quality section
  ✓ Convergence section
  ✓ Recommendations section
  ✓ Summary statistics section

✓ Report saved: snowflake_merge_report.html (142 KB)
✓ Open in browser to view
✓ Print to PDF for distribution
```

**Generated Report Contents:**
```
Executive Summary:
  - 4,940 unified profiles
  - 19,512 total identities
  - 3.95:1 merge ratio
  - 74.7% fragmentation reduction

Identity Resolution:
  - customer_id: 6,489 → 4,940 (23.9% reduction)
  - email: 7,261 → 4,940 (32.0% reduction)
  - phone: 5,762 → 4,910 (14.8% reduction)

Merge Distribution:
  - 89.0% profiles: 3-5 identities (normal)
  - 8.1% profiles: 6-10 identities (high engagement)
  - 2.3% profiles: 10+ identities (complex)

Top Merged Profile:
  - mS9ssBEh4EsN: 38 identities merged

Data Quality:
  - Email: 100% coverage
  - Phone: 99.39% coverage
  - Names: 100% coverage
  - Location: 100% coverage

Expert Recommendations:
  - Implement incremental processing
  - Monitor profiles with 20+ merges
  - Enable downstream activation
  - Set up quality monitoring
```

### Databricks Example

**User Input:**
```
Platform: Databricks
Catalog: customer_cdp
Schema: id_unification
Canonical ID: unified_customer_id
Output: databricks_merge_report.html
```

**Process:**
```
✓ Connected to Databricks (or using Snowflake MCP fallback)
✓ Catalog: customer_cdp.id_unification validated
✓ Tables found:
  - unified_customer_id_lookup
  - unified_customer_id_master_table
  - unified_customer_id_source_key_stats
  - unified_customer_id_result_key_stats

[Same query execution and report generation as Snowflake]

✓ Report saved: databricks_merge_report.html
```

---

## Key Features

### 🎯 **Consistency Guarantee**
- **Same report every time**: Deterministic HTML generation
- **Platform-agnostic design**: Works identically on Snowflake and Databricks
- **Version controlled**: Report structure is fixed and versioned

### 🔍 **Expert Analysis**
- **16 specialized queries**: Comprehensive data collection
- **Calculated metrics**: Deduplication rates, merge ratios, quality scores
- **Pattern detection**: Identify anomalies and outliers
- **Strategic insights**: Actionable recommendations

### 📊 **Professional Visualizations**
- **KPI metric cards**: Large, colorful summary metrics
- **Progress bars**: Coverage percentages with animations
- **Horizontal bar charts**: Distribution comparisons
- **Color-coded badges**: Status indicators (Excellent, Good, Needs Review)
- **Tables with hover effects**: Interactive data exploration

### 🌍 **Platform Flexibility**
- **Snowflake**: Uses `mcp__snowflake__execute_query` tool
- **Databricks**: Uses Databricks MCP tools (with fallback options)
- **Automatic SQL adaptation**: Platform-specific query generation
- **Table name resolution**: Handles catalog vs database differences

### 📋 **Comprehensive Coverage**

**9 Report Sections:**
1. Executive Summary (4 KPIs + findings)
2. Identity Resolution Performance (deduplication analysis)
3. Merge Distribution Analysis (categorized breakdown)
4. Top Merged Profiles (complexity ranking)
5. Source Table Configuration (mappings)
6. Master Table Data Quality (coverage metrics)
7. Convergence Performance (iteration analysis)
8. Expert Recommendations (strategic guidance)
9. Summary Statistics (complete metrics)

**16 Statistical Queries:**
- Source/result key statistics
- Canonical ID counts and distributions
- Merge pattern analysis
- Quality coverage metrics
- Configuration metadata

---

## Table Naming Conventions

The command automatically finds tables based on your canonical ID name:

### Required Tables

For canonical ID = `{canonical_id}`:

1. **Lookup Table**: `{canonical_id}_lookup`
   - Contains: canonical_id, id, id_key_type
   - Used for: Merge ratio, distribution, top profiles

2. **Master Table**: `{canonical_id}_master_table`
   - Contains: {canonical_id}, best_* attributes
   - Used for: Data quality coverage

3. **Source Stats**: `{canonical_id}_source_key_stats`
   - Contains: from_table, total_distinct, distinct_*
   - Used for: Pre-unification baseline

4. **Result Stats**: `{canonical_id}_result_key_stats`
   - Contains: from_table, total_distinct, histogram_*
   - Used for: Post-unification results

### Optional Tables

5. **Unification Metadata**: `unification_metadata`
   - Contains: canonical_id_name, canonical_id_type
   - Used for: Configuration documentation

6. **Column Lookup**: `column_lookup`
   - Contains: table_name, column_name, key_name
   - Used for: Source table mappings

7. **Filter Lookup**: `filter_lookup`
   - Contains: key_name, invalid_texts, valid_regexp
   - Used for: Validation rules

**All tables must be in the same database.schema (Snowflake) or catalog.schema (Databricks)**

---

## Output Format

### HTML Report Features

**Styling:**
- Gradient purple theme (#667eea to #764ba2)
- Modern typography (system fonts)
- Responsive grid layouts
- Smooth hover animations
- Print-optimized media queries

**Sections:**
- Header with gradient background
- Metadata bar with key info
- 9 content sections with analysis
- Footer with generation details

**Visualizations:**
- Metric cards (4 in executive summary)
- Progress bars (6 in data quality)
- Horizontal bar charts (3 throughout report)
- Tables with sorting and hover effects
- Insight boxes with recommendations

**Interactivity:**
- Hover effects on cards and tables
- Animated progress bars
- Expandable insight boxes
- Responsive layout adapts to screen size

### PDF Export

To create a PDF from the HTML report:

1. Open HTML file in browser
2. Press Ctrl+P (Windows) or Cmd+P (Mac)
3. Select "Save as PDF"
4. Choose landscape orientation for better chart visibility
5. Enable background graphics for full styling

---

## Error Handling

### Common Issues and Solutions

**Issue: "Tables not found"**
```
Solution:
1. Verify canonical ID name is correct
2. Check database/catalog and schema names
3. Ensure unification workflow completed successfully
4. Confirm table naming: {canonical_id}_lookup, {canonical_id}_master_table, etc.
```

**Issue: "MCP tools not available"**
```
Solution:
1. For Snowflake: Verify Snowflake MCP server is configured
2. For Databricks: Fall back to Snowflake MCP with proper connection string
3. Check network connectivity
4. Validate credentials
```

**Issue: "No data in statistics tables"**
```
Solution:
1. Verify unification workflow ran completely
2. Check that statistics SQL files were executed
3. Confirm data exists in lookup and master tables
4. Re-run the unification workflow if needed
```

**Issue: "Permission denied"**
```
Solution:
1. Verify READ access to all tables
2. For Snowflake: Grant SELECT on schema
3. For Databricks: Grant USE CATALOG, USE SCHEMA, SELECT
4. Check role/user permissions
```

---

## Success Criteria

Generated report will:

- ✅ **Open successfully** in all modern browsers (Chrome, Firefox, Safari, Edge)
- ✅ **Display all 9 sections** with complete data
- ✅ **Show accurate calculations** for all metrics
- ✅ **Include visualizations** (charts, progress bars, tables)
- ✅ **Render consistently** every time it's generated
- ✅ **Export cleanly to PDF** with proper formatting
- ✅ **Match the reference design** (same HTML/CSS structure)
- ✅ **Contain expert insights** and recommendations
- ✅ **Be production-ready** for stakeholder distribution

---

## Usage Examples

### Quick Start (Snowflake)

```
/cdp-hybrid-idu:hybrid-unif-merge-stats-creator

> Platform: Snowflake
> Database: PROD_CDP
> Schema: ID_UNIFICATION
> Canonical ID: master_customer_id
> Output: (press Enter for default)

✓ Report generated: id_unification_report.html
```

### Custom Output Path

```
/cdp-hybrid-idu:hybrid-unif-merge-stats-creator

> Platform: Databricks
> Catalog: analytics_prod
> Schema: unified_ids
> Canonical ID: td_id
> Output: /reports/weekly/td_id_stats_2025-10-15.html

✓ Report generated: /reports/weekly/td_id_stats_2025-10-15.html
```

### Multiple Environments

Generate reports for different environments:

```bash
# Production
/hybrid-unif-merge-stats-creator
  Platform: Snowflake
  Database: PROD_CDP
  Output: prod_merge_stats.html

# Staging
/hybrid-unif-merge-stats-creator
  Platform: Snowflake
  Database: STAGING_CDP
  Output: staging_merge_stats.html

# Compare metrics across environments
```

---

## Best Practices

### Regular Reporting

1. **Weekly Reports**: Track merge performance over time
2. **Post-Workflow Reports**: Generate after each unification run
3. **Quality Audits**: Monthly deep-dive analysis
4. **Stakeholder Updates**: Executive-friendly format

### Comparative Analysis

Generate reports at different stages:
- After initial unification setup
- After incremental updates
- After data quality improvements
- Across different customer segments

### Archive and Versioning

```
reports/
  2025-10-15_td_id_merge_stats.html
  2025-10-08_td_id_merge_stats.html
  2025-10-01_td_id_merge_stats.html
```

Track improvements over time by comparing:
- Merge ratios
- Data quality scores
- Convergence iterations
- Deduplication rates

---

## Getting Started

**Ready to generate your merge statistics report?**

Please provide:

1. **Platform**: Snowflake or Databricks?
2. **Database/Catalog**: Where are your unification tables?
3. **Schema**: Which schema contains the tables?
4. **Canonical ID**: What's the name of your unified ID? (e.g., td_id)
5. **Output Path** (optional): Where to save the report?

**Example:**
```
I want to generate a merge statistics report for:

Platform: Snowflake
Database: INDRESH_TEST
Schema: PUBLIC
Canonical ID: td_id
Output: my_unification_report.html
```

---

**I'll analyze your ID unification results and create a comprehensive, beautiful HTML report with expert insights!**
