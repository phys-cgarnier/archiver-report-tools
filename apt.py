import sys
import json
import glob
import argparse
from archiver_utility import ArchiverUtility
import pprint
import os
import yaml

class PathGenerator():
    def __init__(self,sub_sys:str = None,loca: str = None)->None:
        self.base_path = '/mccfs2/u1/lcls/epics/ioc/data/'
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='This program creates reports archiver reports given an input of device type (bp for bpm) this program parses *.archive request files to get a list of all PVs that should be archive and checks there status ') 
    parser.add_argument('-o', '--outfile', required = True, help = 'Required argument that is the location and name of outputted results must be .json')
    parser.add_argument('-sub', '--sub_system', help='Subsystem used in wildcard search')
    parser.add_argument('-l', '--loca', help = 'Optional location flag to use in wildcard search, this limits results to just the area of interest')
    parser.add_argument('-d', '--display_mode', action = 'store_true', help = 'Optional argument that displays paths of files to be search but does not search')
    parser.add_argument('-f', '--filename', required = False, help = 'Instead of generating paths use your own list provided from a text file')
    parser.add_argument('-sp', '--save_paths', action='store_true', help = 'Save paths generated to file',  )
    args = parser.parse_args()

    print(f'dump file {args.outfile}')

    if args.filename:
        file_paths = []
        with open(args.filename,'r') as fn:
            lines = fn.readlines()
            for line in lines:
                line = line.strip()
                file_paths.append(line)
            print(file_paths)
    else: 
        generator = PathGenerator(sub_sys=args.sub_system,loca = args.loca)
        file_paths = generator.get_paths()

    if args.display_mode:
        sys.exit(0)

    if args.save_paths:
        with open(f'temp_paths_{args.sub_system}.txt','w') as fn:
            for path in file_paths:
                fn.write(path)
                fn.write('\n')
            fn.close()
   
    utility = ArchiverUtility('lcls')

    master_pv_dictionary = {}
    for f in file_paths:
        temp_pv_list = utility.parse_pvs_from_archive_file(f)
        archive_file = f.rsplit('/',1)[-1]
        #print(archive_file)
        #print(temp_pv_list)
        master_pv_dictionary[archive_file]=temp_pv_list
    #pprint.pprint(master_pv_dictionary)

    print(f'Parsed {len(list(master_pv_dictionary.keys()))} archive files')
    print(f'Preparing to retrieve PV statuses')

    for index, key in enumerate(list(master_pv_dictionary.keys())):
        not_archived_dictionary = {}
        not_archived_list = []
        print(f'In file: {index+1}/{len(list(master_pv_dictionary.keys()))+1},')
        print(f'with filename: {key}')

        master_pv_list = master_pv_dictionary[key]

        for pv in master_pv_list:
            stats= utility.get_pv_status(pv)
            stats_dictionary = stats[0]

            if  stats_dictionary['status'] != 'Being archived':
                not_archived_list.append({stats_dictionary['pvName']:stats_dictionary['status']})

        not_archived_dictionary[key] = not_archived_list
        pprint.pprint(not_archived_dictionary)

        if os.path.exists(args.outfile):
            with open(args.outfile, "r") as f:
                existing_data = yaml.safe_load(f) or {}  # Ensure it's a dictionary
        else:
            existing_data = {}
        
        for key, value in not_archived_dictionary.items():
            if key in existing_data:
                    existing_data[key].extend(value)  # Append to list if key exists
            else:
                existing_data[key] = value  # Add new key

        # Dump everything back to YAML (overwriting with updated data)
        with open(args.outfile, "w") as f:
            yaml.dump(existing_data, f, default_flow_style=False, allow_unicode=True, indent=4, width=200)

