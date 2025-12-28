# Environment Variables Configuration

## Two Options Available

MWAA Local Runner 3.0.6 provides **two options** for environment variables:

1. **`.env` file** - Docker Compose standard (recommended)
2. **`mwaa-local-env` file** - Shell script format (matches 2.10.3 style)

Both files are available in the `docker/` directory. Choose the one that fits your workflow.

## Option 1: `.env` File (Recommended)

Docker Compose automatically reads `.env` files from the same directory where `docker-compose-local.yml` is located. Variables defined in `.env` are automatically available to the `${VARIABLE_NAME}` syntax used in the docker-compose file.

**Format:** `KEY=VALUE` (standard Docker Compose format)
**Usage:** No need to source - Docker Compose reads it automatically

### Setup for `.env` File

1. **Copy the example file:**
   ```bash
   cd docker
   cp .env.example .env
   ```

2. **Edit `.env` with your actual values:**
   ```bash
   # Use your preferred editor
   nano .env
   # or
   vim .env
   # or
   code .env
   ```

3. **Set your credentials and configuration:**
   - AWS credentials (if needed)
   - Snowflake connection details (if needed)
   - Any other custom variables

4. **Start Docker Compose:**
   ```bash
   docker-compose -f docker-compose-local.yml up -d
   ```
   Docker Compose will automatically read the `.env` file.

## Option 2: `mwaa-local-env` File (2.10.3 Style)

This file uses shell script format with `export` statements, matching the MWAA 2.10.3 style exactly.

**Format:** `export KEY=value` (shell script format)
**Usage:** Must be sourced before running docker-compose

### Setup for `mwaa-local-env` File

1. **Edit `mwaa-local-env` with your actual values:**
   ```bash
   cd docker
   nano mwaa-local-env
   # or
   vim mwaa-local-env
   ```

2. **Set your credentials and configuration** (same variables as `.env`)

3. **Source the file and start Docker Compose:**
   ```bash
   source mwaa-local-env
   docker-compose -f docker-compose-local.yml up -d
   ```

## Alternative: Shell Environment Variables

Instead of using a `.env` file, you can also set environment variables in your shell:

```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export SNOWFLAKE_ACCOUNT=your-account
# ... etc

cd docker
docker-compose -f docker-compose-local.yml up -d
```

## How Variables Are Used

In `docker-compose-local.yml`, variables are referenced like this:

```yaml
environment:
  AWS_ACCESS_KEY_ID: "${AWS_ACCESS_KEY_ID}"
  AWS_SECRET_ACCESS_KEY: "${AWS_SECRET_ACCESS_KEY}"
  SNOWFLAKE_ACCOUNT: "${SNOWFLAKE_ACCOUNT:-}"
```

- `${VARIABLE_NAME}` - Reads from `.env` or shell environment
- `${VARIABLE_NAME:-default}` - Uses default value if not set

## Security Notes

⚠️ **Important:** Never commit `.env` files to version control!

1. **Add to `.gitignore`:**
   ```bash
   echo "docker/.env" >> .gitignore
   ```

2. **Use `.env.example` as a template:**
   - Keep `.env.example` in the repository (without secrets)
   - Use it as a template for others
   - Document what variables are needed

## Comparison: 2.10.3 vs 3.0.6

| MWAA 2.10.3 | MWAA 3.0.6 |
|-------------|------------|
| `mwaa-local-env` file (shell script) | **Option 1:** `.env` file (Docker Compose standard) |
| Manually sourced before docker-compose | **Option 1:** Automatically read by Docker Compose |
| Shell script format (`export KEY=value`) | **Option 1:** Standard `.env` format (`KEY=VALUE`) |
| | **Option 2:** `mwaa-local-env` file (same as 2.10.3) |
| | **Option 2:** Manually sourced (same as 2.10.3) |
| | **Option 2:** Shell script format (same as 2.10.3) |

**Note:** MWAA 3.0.6 provides both options. Use `.env` for automatic reading, or `mwaa-local-env` to match the 2.10.3 workflow exactly.

## Example `.env` File

See `.env.example` for a complete template with all available variables.

## Troubleshooting

### Variables Not Being Read

1. **Check file location:**
   - `.env` must be in the same directory as `docker-compose-local.yml`
   - That's the `docker/` directory

2. **Check file format:**
   ```bash
   # Correct format:
   VARIABLE_NAME=value
   
   # Wrong format (no spaces around =):
   VARIABLE_NAME = value  # ❌
   ```

3. **Check for syntax errors:**
   ```bash
   # No quotes needed for simple values:
   AWS_REGION=us-west-2  # ✅
   
   # Quotes needed for values with spaces:
   SOME_VAR="value with spaces"  # ✅
   ```

4. **Verify Docker Compose is reading it:**
   ```bash
   cd docker
   docker-compose -f docker-compose-local.yml config
   # This will show the resolved values
   ```

