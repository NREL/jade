#!/bin/bash
if [ -z $1 ]; then
    echo "run_spark_script.sh requires a command as the first argument"
	exit 1
fi
shift
cmd=(${SPARK_HOME}/$@)
"${cmd[@]}"
