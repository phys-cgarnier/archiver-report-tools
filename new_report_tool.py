
#TODO: last event should only be asked for if specified...
#TODO: add dump file
import argparse
import os
import requests
import pprint
import epics
from typing import List, Dict
import yaml
from collections import OrderedDict
import glob
## TODO: text extract for each subsystem
## TODO: setup helper script to queue up subsystems
## TODO: setup cron to job to call helper script

class ArchiverUtility:
    def __init__(self, mode: str):
        base_urls = {
            "dev": "http://dev-archapp.slac.stanford.edu",
            "lcls": "http://lcls-archapp.slac.stanford.edu",
            "cryo": "http://cryo-archapp.slac.stanford.edu:17665",
            "facet": "http://facet-archapp.slac.stanford.edu"       
        }

        base = base_urls.get(mode, base_urls["dev"])
        if mode not in base_urls:
            print("Warning: Invalid mode provided. Defaulting to 'dev'.")

        self.web = f"{base}/mgmt/bpl/"
        self.retrieval_url = f"{base.replace(':17665', '')}:17668/retrieval/data/"
        self.post_url = f"{base.replace(':17665', '')}/retrieval/data/"

    def get_pv_status(self, pv: str) -> Dict:
        """Request current archiver status for a single PV."""
        url = self.web + "getPVStatus"
        response = requests.get(url, params={'pv': pv})
        response.raise_for_status()
        return response.json()[0]
    
    def parse_archive_file(self, archive_filename: str):
        """Extract PVs and their parameters from a given archive file."""
        pv_list = []
        pv_params_list = []

        with open(archive_filename, 'r') as f:
            for line in f:
                if line.startswith('#') or line.strip() == '':
                    continue
                parts = line.strip().split()
                #pvname = parts[0] if len(parts) > 0 else None
                #pv_params = {'pvname': parts[0], 'scan': parts[1], 'method': parts[2]}
                pv_list.append(parts[0])
                #pv_params_list.append(pv_params)

        return pv_list #, pv_params_list
    
    def get_status(self, pv_list: List[str], **filters) -> Dict[str, Dict]:
        """Retrieve and filter PV status reports."""

        report = {}
        for i, pv in enumerate(pv_list):
            response = self.get_pv_status(pv)
            if response.get("status", "Invalid") not in filters.get("status"): 
                continue
            
            filtered_entry = {pv : {"status": response.get("status")}}
            
            filtered_fields = {
                k: response[k]
                for k, want in filters.items()
                if want is True and k in response and k != "disconnectedStatus"
            }

            filtered_entry[pv].update(filtered_fields)

            if filters.get("disconnectedStatus", None):
                pv_connection = epics.PV(pv)
                if pv_connection.wait_for_connection(timeout=.25):
                    
                   # skip to next pv if we are checking only for disconnected PVs
                   # and the PV is connected, this can be made quicker probably.
                   # but pyepics seems to have limitations with caget_many so I don't
                   # know.
                    continue
                

            report.update(filtered_entry)

        return report

class PathGenerator():
    def __init__(self,sub_sys:str = None,loca: str = None)->None:
        self.base_path = '$IOC_DATA'
        if sub_sys: 
            self.sub_sys = sub_sys
        else:
            self.sub_sys = 'bp'

        if loca:
            self.area = loca
            self.ioc_wildcard_string = '*-{}*-{}*/archive/*.archive'.format(self.area.lower(),self.sub_sys.lower())
        else:
           self.ioc_wildcard_string = '*-*-{}*/archive/*.archive'.format(self.sub_sys.lower()) 

        self.path = self.base_path + self.ioc_wildcard_string
        print(f'Wildcard Path: {self.path}')

    def get_paths(self):
        temp_paths = []
        for file_path in glob.glob(self.path):
            print(file_path)
            temp_paths.append(file_path)
        return temp_paths        

# Functions not in util
def generate_filepaths(subsystem:str):
    generator = PathGenerator(sub_sys = subsystem)
    return generator.get_paths() 

def collect_pvs(args: argparse.Namespace, util: ArchiverUtility):
    """Collect PVs and parameters from provided file or directory."""
    pv_dict = {}
    param_dict = {}

    if args.file:
        pvs = util.parse_archive_file(args.file)
        pv_dict[args.file] = pvs
        #param_dict[args.file] = params

    elif args.directory and os.path.isdir(args.directory):
        for filename in os.listdir(args.directory):
            filepath = os.path.join(args.directory, filename)
            if filepath.endswith('.archive') and os.path.isfile(filepath):
                pvs= util.parse_archive_file(filepath)
                pv_dict[filename] = pvs
                #param_dict[filename] = params
    
    elif args.subsystem:
        filepaths = generate_filepaths(args.subsystem)
        for filepath in filepaths:
            pvs = util.parse_archive_file(filepath)
            filename = os.path.basename(filepath)
            pv_dict[filename] = pvs

    return pv_dict #, param_dict


def setup_search_kwargs(args: argparse.Namespace) -> Dict:
    """Return filtered search kwargs based on CLI options."""
    
    keyword_logic = {
        'Unarchived': lambda: {'status': ['Not being archived']},
        'Paused': lambda: {'status': ['Paused'] },
        'Archived': lambda: {'status': ['Being archived'] },
        'All': lambda: {'status': ['Being archived','Paused','Not being archived'],}
    }

    search_kwargs = keyword_logic[args.keyword]()
    for arg,val in vars(args).items():
        if val == True:
            search_kwargs.update({arg : val})
    return search_kwargs


def build_parser() -> argparse.ArgumentParser:
    """Define and return command-line argument parser."""
    parser = argparse.ArgumentParser(
                    description=("Report tool for PVs in an archive file or folder." 
                    "-f filename or -d dirname, must provide one"))
    
    parser.add_argument("-a", "--archiver", choices=['lcls', 'facet', 'dev', 'cryo'],
                        default = 'lcls',
                        type=str,
                        help= "Optional argument passed for selecting the Archiver to query, default is lcls")

    parser.add_argument("-f", "--file",
                        type=str,
                        help="Path to the archive file")
    
    parser.add_argument("-d", "--directory",
                        type=str,
                        help="Path to a directory  containing archive files.")
    
    parser.add_argument("-sub", "--subsystem",
                        type=str,
                        help="Subsystem to check, pass bp to get all iocs in the wildcard format *-*-bp*")

    parser.add_argument("-k", "--keyword", choices=['Archived', 'Unarchived', 'Paused', 'All'],
                        default = 'All',
                        type=str,
                        help="Reports on the passed status of all PVs, default is all")
    
    parser.add_argument("-ds", "--disconnectedStatus",
                        default=None,
                        action="store_const",
                        const=True,
                        help= "Filter results to show only disconnected PVs in the control system matching all other criteria")
    
    parser.add_argument("-l", "--lastEvent",
                        default = None,
                        action="store_const",
                        const=True,
                        help= ("Optional argument to see the time and date of the last archived event"))
    
    parser.add_argument("-c", "--connectionState",
                        default=None,
                        action="store_const",
                        const=True,
                        help= "Optional argument that displays whether the archiver has connection to the PV")
    return parser

def main():
    parser = build_parser()
    args = parser.parse_args()
    print(args)

    if not args.file and not args.directory and not args.subsystem:
        parser.print_help()
        return

    util = ArchiverUtility(args.archiver)
    
    search_kwargs = setup_search_kwargs(args)
    
    
    pv_dict = collect_pvs(args, util)
    

    
    for filename, pvs_in_file in pv_dict.items():
        print(filename)
        file_report = util.get_status(pvs_in_file, **search_kwargs.copy()) 
        #
        for pv, stats in file_report.items():
            status = stats.get("status", "")
            last_event = stats.get("lastEvent", "")
            conn = stats.get("connectionState", "")

            print(f"{pv:<35}  {status:<18}  {last_event:<28}  {conn}")




if __name__ == "__main__":
    main()