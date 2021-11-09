#!/bin/bash
module load singularity-container
singularity exec \
	-B /scratch:/scratch \
	-B /projects:/projects \
	-B /nopt:/nopt \
	--env SPARK_CONF_DIR=<SPARK_DIR>/conf \
	<CONTAINER_PATH> \
	$@
