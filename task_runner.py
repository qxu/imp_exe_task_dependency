import argparse
import os
import json
import fcntl
import errno
import subprocess
import time
import signal


SIMULATE_TASK_RUNNER_KILLED = False
SIMULATE_TASK_PROCESS_KILLED = False
SIMULATE_SLOW_FINISH = False

CONFIG_FILE = 'task_config.json'

TASK_DIR = 'task'

STARTED_STATUS = 'STARTED'
FINISHED_STATUS = 'FINISHED'
BAD_STATUS = 'BAD'


def get_task_dir(task_id):
    return os.path.join(TASK_DIR, task_id)


def get_workers_path(task_id):
    return os.path.join(get_task_dir(task_id), 'workers')


def get_workers_lock(task_id):
    return os.path.join(get_task_dir(task_id), 'workers.lock')


def make_dir_exist(name):
    try:
        os.makedirs(name)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def flock_nb(fd, operation):
    try:
        fcntl.flock(fd, operation | fcntl.LOCK_NB)
        return True
    except OSError as e:
        if e.errno not in [errno.EACCES, errno.EAGAIN]:
            raise
        else:
            return False


def pid_exists(pid):
    try:
        os.kill(pid, 0)
    except OSError as e:
        if e.errno == errno.ESRCH:
            return False
        if e.errno == errno.EPERM:
            # There's a process (probably), but access was denied
            return True
        raise
    return True



def readline(f, discard_newline=True):
    line = f.readline()
    if line.endswith('\n'):
        return line[:-1]
    else:
        return line


class TaskRunner:
    def __init__(self, task_id, command, dependency_ids):
        self.task_id = task_id
        self.command = command
        self.dependency_ids = dependency_ids
        self.locked = False
        self.workers_lines = None
        self.workers_lock = None
        self.dep_workers_lines = None
        self.dep_workers_locks = {}
        self.bad_dep_workers = None

    def run(self, timeout=10, bad_dep_timeout=4, delay=0.05):
        """
        timeout is the total timeout to verify statuses of dependencies and conflicts.
        bad_dep_timeout is the timeout to wait for a task to set finished status after its pid does not exist.
        Negative timeout means to ignore the timeout.
        """
        start_time = time.time()
        bad_dep_start_times = {}
        run_task = False
        while True:
            self.try_lock_files()

            if self.locked:
                self.load_workers()

                dep_check = self.check_dependencies()
                if dep_check:
                    # Dependencies satisfied, start running task
                    run_task = True
                    break

                if dep_check is not None:
                    # Do not run task because of dependencies
                    print('Failed dependency check')
                    break

                # There are bad workers from the dependencies
                if bad_dep_timeout >= 0:
                    for dep_id, worker_id in self.bad_dep_workers:
                        if (dep_id, worker_id) in bad_dep_start_times:
                            if time.time() - bad_dep_start_times[(dep_id, worker_id)] >= bad_dep_timeout:
                                # timeout on waiting for the bad worker has been reached
                                print('Bad dependency timeout reached, setting BAD_STATUS for', dep_id, worker_id)
                                self.update_dep_worker(dep_id, [BAD_STATUS, str(worker_id)])
                                del bad_dep_start_times[(dep_id, worker_id)]
                        else:
                            print('Waiting for status (pid does not exist) of', dep_id, worker_id)
                            bad_dep_start_times[(dep_id, worker_id)] = time.time()

            self.unlock_files()
            time.sleep(delay)
            if timeout >= 0 and time.time() - start_time >= timeout:
                raise RuntimeError('Timeout exceeded')

        if run_task:
            print('Running', self.task_id)
            max_worker_id = -1
            for line in self.workers_lines:
                row = line.split()
                if row[0] == STARTED_STATUS:
                    worker_id = int(row[1])
                    if worker_id > max_worker_id:
                        max_worker_id = worker_id

            worker_id = max_worker_id + 1
            proc = subprocess.Popen(self.command, shell=True, start_new_session=True)
            self.update_worker([STARTED_STATUS, str(worker_id), str(proc.pid)])
        else:
            print('Not running')

        self.unlock_files()

        if run_task:
            if SIMULATE_TASK_PROCESS_KILLED:
                print('killing', proc.pid)
                os.kill(proc.pid, signal.SIGKILL)

            if SIMULATE_TASK_RUNNER_KILLED:
                print('killing self', os.getpid())
                os.kill(os.getpid(), signal.SIGKILL)
                
            ret_val = proc.wait()

            if SIMULATE_SLOW_FINISH:
                print('Finishing slow')
                time.sleep(3)


            workers_lock = os.open(get_workers_lock(self.task_id), os.O_CREAT | os.O_RDONLY)
            fcntl.flock(workers_lock, fcntl.LOCK_EX)
            self.update_worker([FINISHED_STATUS, str(worker_id), str(ret_val)])
            fcntl.flock(workers_lock, fcntl.LOCK_UN)
            os.close(workers_lock)
            print('Finished')
            

    def try_lock_files(self):
        if self.workers_lock is not None or self.dep_workers_locks:
            raise RuntimeError('Locks already open')

        self.workers_lock = os.open(get_workers_lock(self.task_id), os.O_CREAT | os.O_RDONLY)
        if not flock_nb(self.workers_lock, fcntl.LOCK_EX):
            return

        dep_ids_set = set(self.dependency_ids)
        dep_ids_set.discard(self.task_id)
        for dep_id in dep_ids_set:
            dep_lock = os.open(get_workers_lock(dep_id), os.O_CREAT | os.O_RDONLY)
            self.dep_workers_locks[dep_id] = dep_lock
            if not flock_nb(dep_lock, fcntl.LOCK_EX):
                return

        self.locked = True

    def unlock_files(self):
        self.locked = False

        if self.workers_lock is not None:
            fcntl.flock(self.workers_lock, fcntl.LOCK_UN)
            os.close(self.workers_lock)
            self.workers_lines = None
            self.workers_lock = None

        for dep_lock in self.dep_workers_locks.values():
            fcntl.flock(dep_lock, fcntl.LOCK_UN)
            os.close(dep_lock)
        self.dep_workers_lines = None
        self.dep_workers_locks = {}
        self.bad_dep_workers = None

    def load_workers(self):
        if not self.locked:
            raise RuntimeError('Workers not locked')

        if os.path.isfile(get_workers_path(self.task_id)):
            with open(get_workers_path(self.task_id)) as f:
                self.workers_lines = f.readlines()
        else:
            self.workers_lines = []

        self.dep_workers_lines = {}
        dep_ids_set = set(self.dependency_ids)
        for dep_id in dep_ids_set:
            if os.path.isfile(get_workers_path(dep_id)):
                with open(get_workers_path(dep_id)) as f:
                    self.dep_workers_lines[dep_id] = f.readlines()
            else:
                self.dep_workers_lines[dep_id] = []
        
        self.bad_dep_workers = []

    def update_worker(self, row):
        line = ' '.join(row) + '\n'
        with open(get_workers_path(self.task_id), 'a') as f:
            f.write(line)

    def update_dep_worker(self, dep_id, row):
        line = ' '.join(row) + '\n'
        with open(get_workers_path(dep_id), 'a') as f:
            f.write(line)

    def check_dependencies(self):
        """
        dependencies must have no workers running and the last task
        must have finished successfully
        """

        dependency_check = True

        for dep_id in self.dependency_ids:
            if not self.dep_workers_lines[dep_id]:
                # No workers for the dependency have started yet
                dependency_check = False
                continue

            unfinished_workers = {}

            for line in self.dep_workers_lines[dep_id]:
                rows = line.split()
                if rows[0] == STARTED_STATUS:
                    worker_id = int(rows[1])
                    pid = int(rows[2])
                    if worker_id in unfinished_workers:
                        raise RuntimeError('Invalid worker state')
                    unfinished_workers[worker_id] = pid
                elif rows[0] == FINISHED_STATUS:
                    worker_id = int(rows[1])
                    ret_val = int(rows[2])
                    del unfinished_workers[worker_id]
                elif rows[0] == BAD_STATUS:
                    worker_id = int(rows[1])
                    del unfinished_workers[worker_id]
                else:
                    raise RuntimeError('Invalid status')

            if unfinished_workers:
                for worker_id, pid in unfinished_workers.items():
                    if self.check_bad_pid(dep_id, worker_id, pid):
                        if dependency_check:
                            dependency_check = None
                    else:
                        dependency_check = False

            else:
                # check that the last worker finished with a 0 status
                last_worker_rows = self.dep_workers_lines[dep_id][-1].split()
                if last_worker_rows[0] != FINISHED_STATUS or last_worker_rows[2] != '0':
                    print('Last status not finished 0 for', dep_id)
                    dependency_check = False
                else:
                    # check there are no bad statuses from when the worker started
                    for line in reversed(self.dep_workers_lines[dep_id][:-1]):
                        rows = line.split()
                        if rows[0] == STARTED_STATUS:
                            if rows[1] == last_worker_rows[1]:
                                break
                        elif rows[0] == FINISHED_STATUS:
                            if rows[2] != '0':
                                dependency_check = False
                        elif rows[0] == BAD_STATUS:
                            dependency_check = False
                    else:
                        raise RuntimeError('Unstarted worker')

        return dependency_check

    def check_bad_pid(self, dep_id, worker_id, pid):
        # Worker had started, but if the task was killed, then
        # the status would not have updated
        # Check if the pid exists, not a great solution according to
        # http://mywiki.wooledge.org/ProcessManagement#The_risk_of_letting_the_parent_die
        # but the safer alternative is not viable because we are assuming
        # that the parent process runner can be killed
        if pid_exists(pid):
            return False

        # Assume that the task had finished, mark it as bad
        self.bad_dep_workers.append((dep_id, worker_id))

        return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('task_id')
    parser.add_argument('--kill-runner', action='store_true')
    parser.add_argument('--kill-process', action='store_true')
    parser.add_argument('--slow-finish', action='store_true')
    args = parser.parse_args()

    global SIMULATE_TASK_RUNNER_KILLED
    SIMULATE_TASK_RUNNER_KILLED = args.kill_runner
    global SIMULATE_TASK_PROCESS_KILLED
    SIMULATE_TASK_PROCESS_KILLED = args.kill_process
    global SIMULATE_SLOW_FINISH
    SIMULATE_SLOW_FINISH = args.slow_finish

    task_id = args.task_id

    with open(CONFIG_FILE) as f:
        config = json.load(f)

    task = None

    for config_task in config:
        if config_task['id'] == task_id:
            task = config_task
            break

    if task is None:
        raise RuntimeError('Task id not configured')

    for config_task in config:
        make_dir_exist(get_task_dir(config_task['id']))

    runner = TaskRunner(task['id'], task['command'], task['dependencies'])
    runner.run()


if __name__ == '__main__':
    main()

