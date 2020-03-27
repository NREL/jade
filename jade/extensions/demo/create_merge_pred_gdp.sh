#!/bin/bash

if [ -z $1 ]; then
	echo "Usage: create_merge_pred_gdp.sh AUTO_REGRESSION_OUTPUT"
	exit 1
fi
echo "python jade/extensions/demo/merge_pred_gdp.py run $1" > pred_gdp_commands.txt
jade auto-config generic_command pred_gdp_commands.txt -c config-stage2.json
