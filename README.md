# Task runner with dependency checking

Dependency is satisfied if the runner can guarantee that there are no other workers of that dependency running,
and the last completed task was successful with exit code 0 with no failures from other tasks from when that
task started.

Run with python3

```
python3 task_runner.py -h
usage: task_runner.py [-h] [--kill-runner] [--kill-process] task_id

positional arguments:
  task_id

optional arguments:
  -h, --help      show this help message and exit
  --kill-runner
  --kill-process
  --slow-finish
```

--kill-runner: will kill -9 the task runner
--kill-process: will kill -9 the task command pid
--slow-finish: will sleep before writing the finished status

## task_config.json format
[
    {
        "id": task id,
        "command": command,
        "dependencies": list of dependency task ids
    }
]

## Worker log format
STARTED {worker_id} {process_pid}
FINISHED {worker_id}
BAD {worker_id}

## Notes
Some simplifications
 - Not fully robust, reading and writing worker files assumes that the worker files aren't corrupted. So it doesn't handle a half written line for example.
 - Bad pid checking only happens when checking for dependencies
 - Pids 

