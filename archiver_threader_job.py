import subprocess
import time
import threading
import logging
from datetime import datetime, timezone

abbrev_name_lookup = {
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
    'cf': 'Facilities'

}

subsystems_by_day = {
    'Monday' : ['bp', 'mp', 'tr'], #bpm, machine protection, feedback
    'Tuesday' : ['ws', 'im', 'mg'], #Wirescanner, Toroid, Magnet
    'Wednesday': ['pp', 'bc', 'va'], #Personnel Protection, Beam Containment, Vacuum
    'Thursday': ['ls', 'uc', 'mc'], #Laser, Undulator Control, Motion Control
    'Friday': ['rf', 'tm', 'pm'], #Rf, Temperature, Profile Monitors
    'Saturday': [ 'sp', 'cf' ] #Shared Platform, Facilities
}

logging.basicConfig(
    level=logging.INFO,
    filename="archiver_status.log",
    filemode="a",
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def check_subsystem(subsystem: str):
    start = datetime.now().astimezone()
    name = abbrev_name_lookup.get(subsystem, subsystem)
    logging.info(f"Starting archiver checks for {name} at {start.isoformat()}")

    try:
        subprocess.run(["python", "new_report_tool.py", "-sub", subsystem], check=True)
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

