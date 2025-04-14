
#TODO: test current functionality
#TODO: add disconnection format
#TODO: test for a folder
#TODO: modify report format
#TODO: make code unbreakable
#TODO: make code more readable
import argparse
import os
import requests
import pprint
import epics
from typing import List, Dict


class ArchiverUtility:
    def __init__(self, mode: str):
        base_urls = {
            "dev": "http://dev-archapp.slac.stanford.edu",
            "lcls": "http://lcls-archapp.slac.stanford.edu",
            "cryo": "http://cryo-archapp.slac.stanford.edu:17665"
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
                pv_params = {'pvname': parts[0], 'scan': parts[1], 'method': parts[2]}
                pv_list.append(parts[0])
                pv_params_list.append(pv_params)

        return pv_list, pv_params_list
    
    def get_status(self, pv_list: List[str], disconnected_status: bool = False, **filters) -> Dict[str, Dict]:
        """Retrieve and filter PV status reports."""
        report = {}

        for pv in pv_list:
            response = self.get_pv_status(pv)
            filtered = {
                pv: {
                    k: response[k]
                    for k in filters
                    if k in response and (
                        filters[k] is None or
                        (k == "last_event" and filters[k] in response[k] or k == "last_event" and filters[k] is None) or
                        response[k] == filters[k]
                    )
                }
            }


            if pv in filtered and filtered[pv] != {}:
                if disconnected_status or filters['status'] is None:
                    pv_connection = epics.PV(pv)
                    if not pv.wait_for_connection(timeout=2.5):
                        filtered[pv]['connected pv'] = pv_connection.connected
                report.update(filtered)

        return report




# Functions not in util

def collect_pvs(args: argparse.Namespace, util: ArchiverUtility):
    """Collect PVs and parameters from provided file or directory."""
    pv_dict = {}
    param_dict = {}

    if args.file:
        pvs, params = util.parse_archive_file(args.file)
        pv_dict[args.file] = pvs
        param_dict[args.file] = params

    elif args.directory and os.path.isdir(args.directory):
        for filename in os.listdir(args.directory):
            filepath = os.path.join(args.directory, filename)
            if filepath.endswith('.archive') and os.path.isfile(filepath):
                pvs, params = util.parse_archive_file(filepath)
                pv_dict[filename] = pvs
                param_dict[filename] = params

    return pv_dict, param_dict


def setup_search_kwargs(args: argparse.Namespace) -> Dict:
    """Return filtered search kwargs based on CLI options."""
    keyword_logic = {
        'Unarchived': lambda: {'status': 'Not being archived'},
        'Paused': lambda: {'status': 'Paused', 'last_event': args.last_event},
        'Archived': lambda: {'status': 'Being archived'},
        'All': lambda: {'status': None, 'last_event': args.last_event}
    }

    search_kwargs = keyword_logic[args.keyword]()

    if args.connection_state_archiver is not None or args.keyword == 'All':
        search_kwargs['connectionState'] = args.connection_state_archiver

    return search_kwargs


def build_parser() -> argparse.ArgumentParser:
    """Define and return command-line argument parser."""
    parser = argparse.ArgumentParser(
                    description=("Report tool for PVs in an archive file or folder." 
                    "-f filename or -d dirname, must provide one"))
    
    parser.add_argument("-f", "--file",
                        help="Path to the archive file")
    
    parser.add_argument("-d", "--directory",
                        help="Path to a directory  containing archive files.")
    
    parser.add_argument("-k", "--keyword", choices=['Archived', 'Unarchived', 'Paused', 'All'],
                        default = 'All',
                        help="Reports on the passed status of all PVs, default is all")
    parser.add_argument("-ds", "--disconnected_status", required= False, action="store_true",
                        help= "Filter results to show only disconnected PVs in the control system matching all other criteria")
    parser.add_argument("-a", "--archiver", choices=['lcls', 'facet', 'dev'],
                        default = 'lcls',
                        help= "Optional argument passed for selecting the Archiver to query, default is lcls")
    parser.add_argument("-l", "--last_event", nargs="?",
                        default = None, 
                        help= ("Optional argument passed with Paused to apply additional date filter for last event.\n"
                               "Does not need to be an exact string. example '3/27/25' with filter for last events on that day"))
    
    parser.add_argument("-c", "--connection_state_archiver",
                        type=lambda x: x.lower() == "true" if x.lower() in ("true", "false") else None,
                        choices=[True, False, None], default= None,
                        help= "Optional argument that displays whether the archiver has connection to the PV")
    return parser

def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.file and not args.directory:
        parser.print_help()
        return

    util = ArchiverUtility(args.archiver)
    search_kwargs = setup_search_kwargs(args)
    pv_dict, _ = collect_pvs(args, util)

    for filename, pvs in pv_dict.items():
        statuses = util.get_status(pvs, disconnected_status=args.disconnected_status, **search_kwargs)
        print(f"\n--- Report for: {filename} ---")
        pprint.pprint(statuses)


if __name__ == "__main__":
    main()
