2020-03-19 17:43:17,515 - INFO [jade.cli.run_jobs run_jobs.py:53] : jade-internal run-jobs output/config_batch_3.json --output=output
Traceback (most recent call last):
  File "/home/user/.conda-envs/jade/bin/jade-internal", line 11, in <module>
    load_entry_point('jade', 'console_scripts', 'jade-internal')()
  File "/home/user/.conda-envs/jade/lib/python3.7/site-packages/click/core.py", line 829, in __call__
    return self.main(*args, **kwargs)
  File "/home/user/.conda-envs/jade/lib/python3.7/site-packages/click/core.py", line 782, in main
    rv = self.invoke(ctx)
  File "/home/user/.conda-envs/jade/lib/python3.7/site-packages/click/core.py", line 1259, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/home/user/.conda-envs/jade/lib/python3.7/site-packages/click/core.py", line 1066, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/home/user/.conda-envs/jade/lib/python3.7/site-packages/click/core.py", line 610, in invoke
    return callback(*args, **kwargs)
  File "/home/user/jade/jade/cli/run.py", line 78, in run
    ret = cli.run(config_file, name, output, output_format, verbose)
  File "/home/user/disco/disco/extensions/pydss_simulation/cli.py", line 54, in run
    ret = simulation.run(verbose=verbose)
  File "/home/user/jade/jade/utils/timing_utils.py", line 15, in timed_
    return _timed(func, logger.info, *args, **kwargs)
  File "/home/user/jade/jade/utils/timing_utils.py", line 31, in _timed
    result = func(*args, **kwargs)
  File "/home/user/disco/disco/pydss/pydss_simulation_base.py", line 232, in run
    self._pydss_project.run(logging_configured=False)
  File "/home/user/PyDSS/PyDSS/pydss_project.py", line 262, in run
    inst.run(self._simulation_config, self, scenario)
  File "/home/user/PyDSS/PyDSS/pyDSS.py", line 148, in run
    updated_vis_settings['Simulations']['Generate_visuals'],
  File "/home/user/PyDSS/PyDSS/pyDSS.py", line 182, in __run_scenario
    dss.RunSimulation(project, scenario)
  File "/home/user/PyDSS/PyDSS/dssInstance.py", line 337, in RunSimulation
    self.RunStep(step)
  File "/home/user/PyDSS/PyDSS/dssInstance.py", line 281, in RunStep
    has_converged, error = self._UpdateControllers(priority, step, UpdateResults=False)
  File "/home/user/PyDSS/PyDSS/dssInstance.py", line 216, in _UpdateControllers
    error += controller.Update(Priority, Time, UpdateResults)
  File "/home/user/PyDSS/PyDSS/pyControllers/Controllers/PvController.py", line 91, in Update
    return self.update[Priority]()
  File "/home/user/PyDSS/PyDSS/pyControllers/Controllers/PvController.py", line 270, in VVARcontrol
    if Pcalc > Plim and self.TimeChange is False:
TypeError: '>' not supported between instances of 'float' and 'complex'
2020-03-19 17:44:30,595 - INFO [jade.utils.timing_utils timing_utils.py:34] : execution-time=73.056 s func=run_jobs
slurmstepd: error: *** JOB 2741821 ON node123 CANCELLED AT 2019-11-18T08:24:41 DUE TO TIME LIMIT ***
slurmstepd: error: *** some other error
srun: error: *** some other error
