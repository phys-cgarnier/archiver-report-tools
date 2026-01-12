import subprocess
import time




subsystems_by_day = {
    'Monday' : ['bp', 'mp', 'tr'], #bpm, machine protection, feedback
    'Tuesday' : ['ws', 'im', 'mg'], #Wirescanner, Toroid, Magnet
    'Wednesday': ['pp', 'bc', 'va'], #Personnel Protection, Beam Containment, Vacuum
    'Thursday': ['ls', 'uc', 'mc'], #Laser, Undulator Control, Motion Control
    'Friday': ['rf', 'tm', 'pm'], #Rf, Temperature, Profile Monitors
    'Saturday': [ 'sp', 'cf' ] #Shared Platform, Facilities
}


while True:
    subprocess.run(["python", "new_report_tool.py", "-sub", "bp"], check=True)
    time.sleep(5)