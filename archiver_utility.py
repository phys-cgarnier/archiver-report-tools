import os
import sys
import json
import pprint
import requests
import time
import re
import glob
import datetime

'''
ArchiverUtility: a simple class to access the archiver. Can do almost anything the archiver applicance can do.
To create an ArchiverUtility object, simply declare, for example:
    au = ArchiverUtility("dev") # indicates we are on the dev archiver
    au.load_pvs_from_list("myList", input_list) # loads PVs from a list of strings
    data = au.get_status("myList") # gets status of all PVs in list and returns as a json file
'''
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

        self.pv_lists = {}


    
    def get_data(self, list_name, starttime, endtime, binsize):
        '''Gets all data from a stored list of PV's specified by list_name. Starttime and endtime can be either in string or timestamp format. Binsize is in seconds'''
        try:
            starttime = int(starttime) # It's an int
            startdate = datetime.datetime.fromtimestamp(starttime)
            startdate_string = startdate.strftime('%Y-%m-%dT%H:%M:%S-07:00')
        except ValueError:
            startdate_string = starttime

        try:
            endtime = int(endtime)
            enddate = datetime.datetime.fromtimestamp(endtime)
            enddate_string = enddate.strftime('%Y-%m-%dT%H:%M:%S-07:00')
        except ValueError:
            enddate_string = endtime

        data = {}
        for pv in self.pv_lists[list_name]:
            data[pv] = self.get_pv_data(pv, startdate_string, enddate_string, binsize)
        return data

    
    def get_pv_data(self, pv, starttime, endtime, binsize):
        '''Gets the data for a specific PV'''
        binned_pv = 'mean_' + str(binsize) + '(' + pv + ')'
        
        # Build query for archiver
        try:
            payload = []
            resp = requests.get(self.retrieval_url + "getData.json", params={"pv": binned_pv, "from": starttime, "to": endtime})
            payload = resp.json()
            return payload
        
        except ValueError:
            print("Error!")

    
    def get_data_at_time(self, list_name, time):
        '''Gets the data for a list of PVs at a given time'''
        try:
            time = int(time) # It's an int
            date = datetime.datetime.fromtimestamp(time)
            date_string = date.strftime('%Y-%m-%dT%H:%M:%S-07:00')
        except ValueError:
            date_string = time

        data = {}
        for pv in self.pv_lists[list_name]:
            data[pv] = self.get_pv_data_at_time(pv, date_string)
        return data


    def get_pv_data_at_time(self, pv, time):
        '''Gets the data for a specific PV at a specific time'''
        try:
            time = int(time) # It's an int
            print("it's an int!")
            date = datetime.datetime.fromtimestamp(time)
            date_string = date.strftime('%Y-%m-%dT%H:%M:%S-07:00')
        except ValueError:
            date_string = time

        payload = [pv]
        request_string = self.post_url + 'getDataAtTime?at=' + date_string + '&;includeProxies=false'
        try:
            resp = requests.post(request_string, json=[pv])
            return resp.json()
        
        except ValueError:
            print("Error!")



    def pausePVs(self, list_name, keepdata=False):
        '''Pauses the list of PV's specified by list_name'''
        for pv in self.pv_lists[list_name]:
            pvParams = (("pv", pv), ("deleteData", "true" if keepdata else "false"))
            print(pvParams)
            pausePVResponse = self.pausePV(pv)
            if pausePVResponse.status_code != requests.codes.ok:
                print("{} returned status code {} retrieving data for {}".format(parser.prog, pausePVResponse.status_code, pv))
                    #print( 'Status code : ', deletePVResponse.status_code, ' returned for ', pv)
            else:
                print(pausePVResponse.text)


    def deletePV(self, pvParams):
        '''Deletes the PV specified by pvName'''
        url = self.web + '/deletePV'
        deletePVResponse = requests.get(url, params=pvParams)
        return deletePVResponse


    def pausePV(self, pv):
        '''Pauses the archiving pv'''
        url = self.web + '/pauseArchivingPV'
        payload = {'pv': pv}
        pausePVResponse = requests.get(url, payload)
        return pausePVResponse

    
    def getAllPausedPVs(self):
        '''Returns a json of all paused PV's'''
        paused = self.getPaused()
        pausedPVList = paused.json()
        return pausedPVList


    def getPaused(self):
        '''Gets a list of paused PV's'''
        url = self.web + 'getPausedPVsForThisAppliance'
        getPaused = requests.get(url)
        getPaused.raise_for_status() 
        if getPaused.status_code != requests.codes.ok:
            print(getPaused.status_code) 
        else:
            return getPaused

    
    def getAllDisconnectedPVs(self):
        '''Returns a json of all disconnected PV's'''
        pvList = []
        disc = self.getDisconnects()
        discStatsList = disc.json()
        for item in discStatsList:
            pvList.append(item['pvName'])
        pvList.sort()
        return pvList

    
    def getDisconnects(self):
        '''Gets the currently disconnected PV's'''
        url = self.web + 'getCurrentlyDisconnectedPVs'
        getDisc = requests.get(url)
        getDisc.raise_for_status() 
        if getDisc.status_code != requests.codes.ok:
            print(getDisc.status_code) 
        else:
            return getDisc

    
    def resamplePVs(self, list_name, samplingPeriod, samplingMethod):
        '''Resamples all PV's in the given list to the given sampling period (in seconds) and sampling method (either 'MONITOR' or 'SCAN').'''
        if (samplingMethod != 'MONITOR' and samplingMethod != 'SCAN'):
            print("Error - samplingMethod must be 'MONITOR' or 'SCAN'")
            return

        lines = []
        pvParamList = []
        pvParamKeys = ['pv', 'samplingperiod', 'samplingmethod']
        for pv in self.pv_lists[list_name]:
            pvParamVals = [pv, samplingPeriod, samplingMethod]
            pvParamDict = {key:value for (key, value) in zip(pvParamKeys, pvParamVals)}
            pvParamList.append(pvParamDict)
            pvChangeParamsResponse = self.changeArchivalParameters(pvParamDict)
            #print('Response returned with status ', pvChangeParamsResponse)


    def resamplePV(self, pv, samplingPeriod, samplingMethod):
        '''Resamples a single PV to the given sampling period (in seconds) and sampling method (either 'MONITOR' or 'SCAN').'''
        pvParamList = []
        pvParamKeys = ['pv', 'samplingperiod', 'samplingmethod']

        pvParamVals = [pv, samplingPeriod, samplingMethod]
        pvParamDict = {key:value for (key, value) in zip(pvParamKeys, pvParamVals)}
        pvParamList.append(pvParamDict)
        pvChangeParamsResponse = self.changeArchivalParameters(pvParamDict)
        #print('Response returned with status ', pvChangeParamsResponse)

    
    def changeArchivalParameters(self, pvParams):
        '''Changes the archival parameters using pvParams'''
        url = self.web + '/changeArchivalParameters'
        resp = requests.get(url, params=pvParams)
        return resp.status_code

    @staticmethod
    def parse_pvs_from_archive_file(archive_filename):
        pv_list = []
        with open(archive_filename,'r') as f:
            lines = f.readlines() 
            for line in lines:
                #print(line)
                if line.startswith('#'):
                    continue
                if line.startswith('\n'):
                    continue
                else:
                    line = line.split(' ', 1)[0]
                    line = line.strip('\n')
                    line = line.split('\t',1)[0]
                    pv_list.append(line)
        return pv_list

    @staticmethod
    def parse_pvs_and_params_from_archive_file(archive_filename):
        pv_list = []
        pv_params_list = []
        with open(archive_filename,'r') as f:
            lines = f.readlines() 
            for line in lines:
                #print(line)
                if line.startswith('#'):
                    continue
                if line.startswith('\n'):
                    continue
                else:
                    print(line)
                    line = line.strip('\n')
                    parts = line.split(' ')
                    print(parts)
                    pv_params = {'pvname': parts[0], 'scan': parts[1], 'method': parts[2]}
                    pv_params_list.append(pv_params)
                    pv_list.append(parts[0])
        return pv_list, pv_params
    


    def get_status(self,pv_list:list[str])->dict[str,dict]:
        '''Gets all statuses of a stored list of PV's specified by list_name'''
        data = {}
        for pv in pv_list:
            data[pv] = self.get_pv_status(pv)
        return data

    def get_pv_status(self, pv):
        '''Gets the status of a specific PV'''
        payload = {'pv': pv}
        url = self.web + "getPVStatus"

        get_stats = requests.get(url, params=payload)
        get_stats.raise_for_status()

        if get_stats.status_code == requests.codes.ok:
            stats = get_stats.json()
            return stats[0]