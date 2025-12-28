# Testing Checklist for MWAA Local Runner 3.0.6

Use this checklist to verify everything is working correctly.

## Pre-Flight Checks ✅

### 1. File Structure Validation
- [x] All required folders exist (dags, docker, requirements, plugins, secrets, startup_script)
- [x] Sample DAGs are in `dags/samples/` folder
- [x] `requirements.txt` is clean and contains only libraries
- [x] `.gitignore` properly configured

### 2. Code Validation
- [x] No linter errors in DAG files
- [x] All DAGs use Airflow 3.0 syntax (`schedule` not `schedule_interval`)
- [x] No deprecated patterns (`execution_date`, `SimpleHttpOperator`, `provide_context`)
- [x] Docker compose file syntax is valid

## Docker Setup Testing

### 3. Docker Image Check
```bash
# Verify Docker image exists
docker images | grep "amazon-mwaa-docker-images/airflow:3.0.6-dev"
```
- [ ] Docker image is present

### 4. Start Containers
```bash
cd docker
docker-compose -f docker-compose-local.yml up -d
```
- [ ] Containers start without errors

### 5. Check Container Status
```bash
docker-compose -f docker-compose-local.yml ps
```
Expected output:
- [ ] `postgres` - Running (healthy)
- [ ] `migratedb` - Exited (completed successfully)
- [ ] `webserver` - Running
- [ ] `scheduler` - Running

### 6. Check Container Logs
```bash
# Check webserver logs
docker-compose -f docker-compose-local.yml logs webserver | tail -20

# Check scheduler logs
docker-compose -f docker-compose-local.yml logs scheduler | tail -20
```
- [ ] No critical errors in logs
- [ ] Webserver started successfully
- [ ] Scheduler started successfully

## Airflow UI Testing

### 7. Access Airflow UI
- [ ] Open http://localhost:8081
- [ ] Login with `airflow` / `airflow`
- [ ] UI loads without errors

### 8. Verify DAGs Appear
Wait 30-60 seconds for scheduler to discover DAGs, then check:
- [ ] `sample_dag_mwaa_3_0` appears in DAG list
- [ ] `sample_taskflow_api_dag` appears in DAG list
- [ ] `sample_airflow_3_syntax_dag` appears in DAG list
- [ ] No parsing errors shown

### 9. Check DAG Details
For each sample DAG:
- [ ] Click on DAG name
- [ ] Graph view loads correctly
- [ ] Task dependencies are visible
- [ ] No errors in DAG details

## DAG Execution Testing

### 10. Test Basic DAG (`sample_dag_mwaa_3_0`)
1. Unpause the DAG (toggle switch)
2. Trigger a manual run (play button)
3. Monitor execution:
   - [ ] All 3 tasks complete successfully
   - [ ] Task logs are accessible
   - [ ] No task failures

### 11. Test TaskFlow API DAG (`sample_taskflow_api_dag`)
1. Unpause the DAG
2. Trigger a manual run
3. Monitor execution:
   - [ ] All 3 tasks complete successfully
   - [ ] XCom data passes between tasks correctly
   - [ ] Task logs show correct data flow

### 12. Test Syntax DAG (`sample_airflow_3_syntax_dag`)
1. Unpause the DAG
2. Trigger a manual run
3. Monitor execution:
   - [ ] All 4 tasks complete successfully
   - [ ] Task logs show correct date handling
   - [ ] No syntax errors

## Requirements Testing

### 13. Verify Package Installation
```bash
# Check if packages are installed in container
docker-compose -f docker-compose-local.yml exec scheduler pip list | grep -E "pendulum|boto3|snowflake|mysql"
```
- [ ] `pendulum` is installed
- [ ] `boto3` is installed
- [ ] `apache-airflow-providers-snowflake` is installed
- [ ] `apache-airflow-providers-mysql` is installed

## Configuration Testing

### 14. Environment Variables (if using .env)
- [ ] `.env` file exists in `docker/` directory (if needed)
- [ ] Variables are read correctly by containers
- [ ] No undefined variable errors

### 15. Execution API
- [ ] Execution API is accessible (webserver running)
- [ ] JWT secret is configured
- [ ] No API connection errors in scheduler logs

## Cleanup Testing

### 16. Stop Containers
```bash
cd docker
docker-compose -f docker-compose-local.yml down
```
- [ ] Containers stop cleanly

### 17. Restart Test
```bash
docker-compose -f docker-compose-local.yml up -d
```
- [ ] Containers restart successfully
- [ ] DAGs still appear correctly
- [ ] No data loss (if not using `-v` flag)

## Troubleshooting

If any step fails:

1. **Check logs:**
   ```bash
   docker-compose -f docker-compose-local.yml logs
   ```

2. **Verify Docker image:**
   ```bash
   docker images | grep "amazon-mwaa-docker-images/airflow:3.0.6-dev"
   ```

3. **Check ports:**
   ```bash
   lsof -i :8081  # Should show webserver
   lsof -i :5433  # Should show postgres
   ```

4. **Reset database (if needed):**
   ```bash
   docker-compose -f docker-compose-local.yml down -v
   docker-compose -f docker-compose-local.yml up -d
   ```

## Success Criteria

All tests pass if:
- ✅ All containers start and run
- ✅ Airflow UI is accessible
- ✅ All 3 sample DAGs appear without errors
- ✅ All sample DAGs execute successfully
- ✅ All required packages are installed
- ✅ No critical errors in logs

## Notes

- First startup may take 1-2 minutes for database migration
- DAG discovery takes 30-60 seconds
- Some logs may show warnings (these are usually safe to ignore)
- Database data persists in `db-data/` folder (excluded from git)

