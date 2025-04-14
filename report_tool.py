import argparse
import os
import requests 
import pprint
import epics

class ArchiverUtility:
    def __init__(self, mode):
        if (mode == "dev"):
            self.web = "http://dev-archapp.slac.stanford.edu/mgmt/bpl/"
            self.retrieval_url = 'http://dev-archapp.slac.stanford.edu:17668/retrieval/data/'
            self.post_url = 'http://dev-archapp.slac.stanford.edu/retrieval/data/'
        elif (mode == "lcls"):
            self.web = "http://lcls-archapp.slac.stanford.edu/mgmt/bpl/"
            self.retrieval_url = 'http://lcls-archapp.slac.stanford.edu:17668/retrieval/data/'
            self.post_url = 'http://lcls-archapp.slac.stanford.edu/retrieval/data/'
        elif (mode == "cryo"):
            self.web = "http://cryo-archapp.slac.stanford.edu:17665/mgmt/bpl/"
            self.retrieval_url = 'http://cryo-archapp.slac.stanford.edu:17668/retrieval/data/'
            self.post_url = 'http://cryo-archapp.slac.stanford.edu/retrieval/data/'
        else:
            print("Error: Mode must be either 'dev' or 'lcls.' Defaulting to 'dev.'")
            self.web = "http://dev-archapp.slac.stanford.edu/mgmt/bpl/"
            self.retrieval_url = 'http://dev-archapp.slac.stanford.edu:17668/retrieval/data/'
            self.post_url = 'http://dev-archapp.slac.stanford.edu/retrieval/data/'

    def parse_pvs_and_params_from_archive_file(self, archive_filename:str):
        pv_list = []
        pv_params_list = []
        with open(archive_filename,'r') as f:
            lines = f.readlines() 
            for line in lines:
                if line.startswith('#'):
                    continue
                if line.startswith('\n'):
                    continue
                else:
                    line = line.strip('\n')
                    parts = line.split(' ')
                    pv_params = {'pvname': parts[0], 'scan': parts[1], 'method': parts[2]}
                    pv_params_list.append(pv_params)
                    pv_list.append(parts[0])
        return pv_list, pv_params_list
    
    def get_status(self, pv_list:list[str], disconnected_status: bool = False,  **kwargs)->dict[str,dict]:
        '''Gets all statuses of a stored list of PV's 
          returns a report specific by kwargs for each pv '''
        report = {}
        for pv in pv_list:
            response =  self.get_pv_status(pv)
            #pprint.pprint(response)
            report_items = {pv : {k: v for k, v in response.items() 
                            if k in kwargs and (kwargs[k] is None or response[k] == kwargs[k])}
            }
            if report_items[pv] != {}:
                if disconnected_status:
                    disc_stat = epics.PV(pv)
                    #print(disc_stat)
                    #pprint.pprint(report_items)
                    report_items[pv].update({'connected pv': disc_stat.connected})
                else:
                    pass    
                report.update(report_items)
        return report

    def get_pv_status(self, pv:str):
        '''Gets the status of a specific PV'''
        payload = {'pv': pv}
        url = self.web + "getPVStatus"
        get_stats = requests.get(url, params=payload)
        get_stats.raise_for_status()
        if get_stats.status_code == requests.codes.ok:
            stats = get_stats.json()
            return stats[0]


def setup_search_kwargs(args: argparse.Namespace):
    #print(type(args))
    search_kwargs = {}
    # this will have to be thoroughly checked
    if args.keyword == 'Unarchived':
        search_kwargs.update({'status': 'Not being archived'})
        return search_kwargs
    elif args.keyword == 'Paused':
        search_kwargs.update({'status': 'Paused'})
        search_kwargs.update({'last_event': args.last_event}) 
    elif args.keyword == 'Archived':
        search_kwargs.update({'status': 'Being archived'})

    else:
        search_kwargs.update({'status': None})
        search_kwargs.update({'last_event': args.last_event}) 

    if args.connection_state_archiver is not None:
        search_kwargs.update({'connectionState': args.connection_state_archiver})

    return search_kwargs
    

def main():
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
    

    args = parser.parse_args()
    if not args.file and not args.directory:
        parser.print_help()
    pv_dict = {}
    pv_params_dict = {}
    search_kwargs = setup_search_kwargs(args)
    #print(search_kwargs)
    util = ArchiverUtility(args.archiver)


    if args.file:
        pvs, params = util.parse_pvs_and_params_from_archive_file(args.file)
        pv_dict.update({args.file : pvs})
        pv_params_dict.update({args.file : params})
    elif args.directory:
        if os.path.isdir(args.directory):
            for filename in os.listdir(args.directory):
                filepath = os.path.join(args.directory, filename)
                if os.path.isfile(filepath) and filepath.endswith('.archive'):
                    pvs, params = util.parse_pvs_and_params_from_archive_file(args.file)
                    pv_dict.update({filename : pvs})
                    pv_params_dict.update({filename: params})

    pv_statuses_dict = {}
    for filename, pvs in pv_dict.items():
        status = util.get_status(pvs, disconnected_status= args.disconnected_status, **search_kwargs)
        pprint.pprint(status)

if __name__ == "__main__":
    main()