


"""
Daily Archiver QA Scheduler

This module schedules and executes daily EPICS archiver quality-assurance checks
for LCLS subsystems. Subsystems are grouped by day of the week and executed either
sequentially or with limited parallelism. Each subsystem run invokes an external
reporting tool and logs timing, status, and failures to a persistent log file.

Key features
------------
- Day-of-week subsystem scheduling
- Local-time-based execution semantics (e.g. "run at 1am")
- Optional limited parallel execution using threads
- Robust logging of start/end times, duration, and exceptions

Assumptions
-----------
- The system clock timezone matches the desired operational timezone.
- `new_report_tool.py` is available on PATH or invoked relative to the working directory.
- Log directory exists and is writable.
"""



import subprocess
import time
import threading
import logging
from datetime import datetime, timedelta


"""
Mapping from subsystem abbreviations to human-readable subsystem names.

Keys
----
str
    Short subsystem identifiers found as part of subsystem IOC names $IOC_DATA

Values
------
str
    Subsystem Names

Notes
-----
This mapping is primarily used for log readability. If an abbreviation is not
present, the raw abbreviation will be logged instead.
"""

abbrev_name_lookup = {
    'ky': 'Klystron'
    'bp': 'BPM',
    'mp': 'Machine Protection System',
    'tr': 'Feedback',
    'ws': 'WireScanner',
    'im': 'Toroid',
    'mg': 'Magnet',
    'pp': 'Personnel Protection',
    'bc': 'Beam Containment', 
    'va': 'Vacuum',
    'ls': 'Laser',
    'uc': 'Undulator Control', 
    'mc': 'Motion Control',
    'rf': 'All RF',
    'tm': 'Temperature', 
    'pm': 'Profile Monitors',
    'sp': 'Shared Platform',
    'cf': 'Facilities',
    'ex': 'ex?',
    'cv': 'Cv?',
    'gd': 'gd?',
    'bl': 'bl?'


}

"""
Mapping of weekdays to the subsystems scheduled for archiver QA checks.

Keys
----
str
    Day of week name as returned by `strftime("%A")`
    (e.g. 'Monday', 'Tuesday').

Values
------
list[str]
    List of subsystem abbreviations to be checked on that day.

Notes
-----
- Scheduling is purely calendar-based and does not persist state.
- Subsystems listed here are executed once per day when scheduled.
"""


subsystems_by_day = {
    'Monday' : ['bp', 'mp', 'tr', 'bl'], #bpm, machine protection, feedback, bl?
    'Tuesday' : ['ws', 'im', 'mg', 'gd'], #Wirescanner, Toroid, Magnet, gd?
    'Wednesday': ['pp', 'bc', 'va'], #Personnel Protection, Beam Containment, Vacuum
    'Thursday': ['ls', 'uc', 'mc'], #Laser, Undulator Control, Motion Control
    'Friday': ['rf', 'tm', 'pm'], #Rf, Temperature, Profile Monitors
    'Saturday': [ 'ky', 'sp', 'cf'], #Klystron, Shared Platform, Facilities,
    'Sunday': ['rd', 'rc', 'cv', 'ex'] #idk?
}

logging.basicConfig(
    level=logging.INFO,
    filename="/var/log/lcls-archiver-qa.log",
    filemode="a",
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def local_now():
    """
    Return the current local time as a timezone-aware datetime.

    Returns
    -------
    datetime
        Current datetime with the system's local timezone applied.
    """
    return datetime.now().astimezone()

def next_run_time(hour: int = 1, minute: int = 0) -> datetime:
    """
    Compute the next scheduled run time at a fixed local hour and minute.

    Parameters
    ----------
    hour : int, optional
        Hour of day (0–23) when the job should run, by default 1.
    minute : int, optional
        Minute of the hour when the job should run, by default 0.

    Returns
    -------
    datetime
        Timezone-aware datetime representing the next scheduled run time.

    Notes
    -----
    - If the target time today has already passed, the next run will be
      scheduled for the following day.
    - Seconds and microseconds are always set to zero.
    """
    now = local_now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target

def run_today_subsystems(max_parallel: int = 1):
    
    """
    Execute all subsystems scheduled for the current day.

    Parameters
    ----------
    max_parallel : int, optional
        Maximum number of subsystems to run concurrently.
        - 1 executes subsystems sequentially (default, safest).
        - Values >1 enable limited parallel execution using threads.

    Notes
    -----
    - Subsystems are selected based on the current local weekday.
    - Parallel execution uses a semaphore to cap concurrency.
    - This function blocks until all scheduled subsystem checks complete.
    """

    today = local_now().strftime("%A")  # 'Monday', 'Tuesday', ...
    subsystems = subsystems_by_day.get(today, [])

    if not subsystems:
        logging.info(f"No subsystems scheduled for {today}.")
        return

    logging.info(f"Scheduled subsystems for {today}: {subsystems}")

    if max_parallel <= 1:
        # Sequential (simplest, safest)
        for sub in subsystems:
            check_subsystem(sub)
    else:
        # Limited parallelism using threads
        sem = threading.Semaphore(max_parallel)
        threads = []

        def wrapped(sub):
            with sem:
                check_subsystem(sub)

        for sub in subsystems:
            t = threading.Thread(target=wrapped, args=(sub,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

def check_subsystem(subsystem: str):
    """
    Run the archiver QA check for a single subsystem.

    Parameters
    ----------
    subsystem : str
        Subsystem abbreviation identifying the subsystem to check.

    Notes
    -----
    - Executes `new_report_tool.py` as a subprocess.
    - Logs start time, end time, execution duration, and success/failure.
    - Any raised exception is logged with full traceback.
    """

    start = datetime.now().astimezone()
    name = abbrev_name_lookup.get(subsystem, subsystem)
    logging.info(f"Starting archiver checks for {name} at {start.isoformat()}")

    try:
        subprocess.run(["python", "new_report_tool.py", "-sub", subsystem, '-k', 'UP', '-l', '--dump'], check=True)
        status = "success"
    except Exception as e:
        status = "error"
        exception_time = datetime.now().astimezone()
        logging.exception(
            f"Runtime Error at {exception_time.isoformat()} when running checks for {name}: {e}"
        )

    finish = datetime.now().astimezone()
    duration_s = (finish - start).total_seconds()
    logging.info(
        f"Archiver checks for {name} finished at {finish.isoformat()} "
        f"(status={status}, duration={duration_s:.2f}s)"
    )

def scheduler_loop(run_hour: int = 1, run_minute: int = 0, max_parallel: int = 1):
    """
    Main scheduler loop that triggers daily subsystem checks.

    Parameters
    ----------
    run_hour : int, optional
        Hour of day (0–23) at which the daily run should start, by default 1.
    run_minute : int, optional
        Minute of the hour when the run should start, by default 0.
    max_parallel : int, optional
        Maximum number of subsystems to execute concurrently.

    Notes
    -----
    - This function runs indefinitely.
    - Uses local time to compute sleep duration until the next run.
    - Intended to be invoked as a long-running process or service.
    """
    logging.info(f"Scheduler started. Will run at {run_hour:02d}:{run_minute:02d} local time.")
    while True:
        run_at = next_run_time(run_hour, run_minute)
        sleep_s = (run_at - local_now()).total_seconds()
        logging.info(f"Next run at {run_at.isoformat()} (sleeping {sleep_s:.0f}s)")
        time.sleep(max(0, sleep_s))

        started = local_now()
        logging.info(f"=== Daily run triggered at {started.isoformat()} ===")
        run_today_subsystems(max_parallel=max_parallel)
        finished = local_now()
        logging.info(f"=== Daily run completed at {finished.isoformat()} ===")

if __name__ == "__main__":
    # max_parallel=1 => sequential; bump to 2 or 3 if you want to overlap subsystem runs (not tested extensively)
    scheduler_loop(run_hour=1, run_minute=0, max_parallel=1)
