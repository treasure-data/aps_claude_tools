# CDP Tools Examples and Templates

This directory contains sanitized example configurations and templates for the CDP Tools Marketplace.

## Available Templates

### 1. unify.yml.example
Example ID unification configuration file with generic database/table names.

**Usage:**
```bash
# Copy to root and customize for your project
cp examples/unify.yml.example unify.yml

# Update with your actual database and table names
# Replace 'demo_db' with your database name
# Replace table names with your actual staging tables
```

## Database Naming Conventions

The examples use generic, client-agnostic naming patterns:

### Database Names
- `client_src` - Source/raw data database
- `client_stg` - Staging/transformed data database
- `client_unification` - ID unification database
- `demo_db` - Generic demo database for examples

### Table Names
- `{source}_{object}_histunion` - Combined historical + incremental tables
- `{source}_{object}` - Staging transformed tables
- `customer_orders` - Generic order table example
- `customer_profiles` - Generic profile staging table

## Customizing for Your Project

When adapting these examples for a specific client:

### Step 1: Choose a Client Prefix
Replace generic names with client-specific prefix:
```
client_src → acme_src
client_stg → acme_stg
demo_db → acme_demo
```

### Step 2: Update Database Names
Edit configuration files to use your database names:
```yaml
# Before (generic)
database: client_src

# After (client-specific)
database: acme_src
```

### Step 3: Update Table Names
Use descriptive table names that match your data sources:
```yaml
# Before (generic)
table: customer_orders

# After (source-specific)
table: shopify_orders
```

## Best Practices

1. **Use Consistent Prefixes**
   - All databases for a client should share the same prefix
   - Example: `acme_src`, `acme`, `acme_unification`

2. **Include Source System Names**
   - Table names should indicate the source system
   - Example: `klaviyo_events`, `shopify_products`, `salesforce_leads`

3. **Use Descriptive Suffixes**
   - `_hist` - Historical full-load data
   - `_histunion` - Combined historical + incremental
   - `` - Clean, transformed data
   - `_master` - Master unified tables

4. **Avoid Personal Names**
   - Don't use developer names in examples
   - Use role-based or generic names instead
   - Example: `test_orders` not `john_orders`

## Example: Complete Naming Pattern

For a client "Acme Corp" with Shopify and Klaviyo data:

```
Databases:
- acme_src          (raw ingested data)
- acme      (transformed data)
- acme_unification  (unified customer profiles)

Tables in acme_src:
- shopify_products
- shopify_products_hist
- shopify_products_histunion
- klaviyo_events
- klaviyo_events_hist
- klaviyo_events_histunion

Tables in acme:
- shopify_products
- klaviyo_events
- customer_orders

Tables in acme_unification:
- acme_id_master_table
- acme_id_mapping
```

## Template Usage in Plugins

All CDP plugins use these naming conventions in their generated examples. When using slash commands, you can provide your actual database/table names and the plugins will generate appropriate configurations.

---

For more information, see the main README.md in the repository root.
