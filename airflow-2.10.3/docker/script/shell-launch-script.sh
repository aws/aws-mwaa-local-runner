#!/usr/bin/env bash

timeout_seconds=300
exec 3>&1 4>&2
time=$(TIMEFORMAT="%R"; { time timeout "${timeout_seconds}s" bash /run-startup.sh 1>&3 2>&4; } 2>&1)
exit_status=$?
exec 3>&- 4>&-
if [ $exit_status -eq 124 ]; then
 echo "Startup script timed out after $timeout_seconds seconds."
else
 echo "Finished running startup script. Execution time: ${time}s."
 source stored_env
fi
echo "Running verification"
bash /verification.sh
echo "Verification completed"