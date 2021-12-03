#!/bin/bash
module load singularity-container

env_vars=""
if [ ! -z $SPARK_CONF_DIR ]; then
    env_vars="$env_vars --env SPARK_CONF_DIR=$SPARK_CONF_DIR"
fi
if [ ! -z $SPARK_LOG_DIR ]; then
    env_vars="$env_vars --env SPARK_LOG_DIR=$SPARK_LOG_DIR"
fi

singularity exec \
	-B /scratch:/scratch \
	-B /projects:/projects \
	-B /lustre:/lustre \
	-B /nopt:/nopt \
    $env_vars \
	<CONTAINER_PATH> \
	<SPARK_DIR>/bin/run_spark_script.sh $@
