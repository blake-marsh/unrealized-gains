#-----------------
# Get H.15 Yields
#-----------------
import sys,os, tempfile
from pathlib import Path 

#------------------
# set up log files
#------------------
old_stdout = sys.stdout
log_file_name=os.path.join(os.path.expanduser("~/unrealized-gains/logfiles"),'download_h15.log')
log_file = open(log_file_name,"w")
sys.stdout = log_file

#------------------------------------------------
print("Download H.15 datasets" + "\n")
#------------------------------------------------
# Log time stamp
import datetime
print("Log created on " + str(datetime.datetime.now()))

## load modules
import os, urllib, urllib3, requests, zipfile
from bs4 import BeautifulSoup
import pandas as pd
import lxml.etree as ET
import numpy as np
from pandas.tseries.offsets import MonthEnd

#-------------------
# path to save data
#-------------------
datapath = os.path.expanduser('~/unrealized-gains/data/')

#---------------------------
# data download function
#---------------------------

def data_download(dataname, urlpath, datapath, filename, headers=None):

    ## print user provided dataname
    print("---------------------------------------------------------\n" + "Downloading " + dataname + "\n" + "---------------------------------------------------------\n")

    ## print start time of download
    print("Download Start: " + str(datetime.datetime.now()) + "\n")

    ## Print url path
    print("Downloading From URL: " + urlpath + "\n")

    ## set working directory
    os.chdir(datapath)
    datapath = os.getcwd()
    print("Dataset stored at: " + datapath + "\n")

    ## add headers if needed
    if headers != None:
        opener = urllib.request.build_opener()
        opener.addheaders = headers
        urllib.request.install_opener(opener)

    ## start download
    try:
        saveto = datapath + "/" + filename
        urllib.request.urlretrieve(urlpath, saveto)
        print("Saved dataset " + dataname + " as: " + filename + "\n")
    except:
        print("Unable to download dataset: " + dataname)

    ## print end time
    print("Download Ended: " + str(datetime.datetime.now()) + "\n")
    print("---------------------------------------------------------\n")


#------------------------------------
# Function for processing XML files
# downloaded from the FRB DDP
#------------------------------------

def DDP_XML(FileToUnzip, XMLToRead, datapath):

    ## Unzip file
    print(os.getcwd())
    zip_ref = zipfile.ZipFile(os.path.join(datapath, FileToUnzip), 'r')
    zip_ref.extractall(datapath)
    zip_ref.close()

    ## define XML file
    XMLpath = os.path.join(datapath, XMLToRead)

    def fast_iter(context, func, *args, **kwargs):
        """
        http://lxml.de/parsing.html#modifying-the-tree
        Based on Liza Daly's fast_iter
        http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
        See also http://effbot.org/zone/element-iterparse.htm
        http://stackoverflow.com/a/7171543/190597 (unutbu)
        """
        for event, elem in context:
            func(elem, *args, **kwargs)
            # It's safe to call clear() here because no descendants will be
            # accessed
            elem.clear()
            # Also eliminate now-empty references from the root node to elem
            for ancestor in elem.xpath('ancestor-or-self::*'):
                while ancestor.getprevious() is not None:
                    del ancestor.getparent()[0]
        del context

    data = list()
    obs_keys = ['OBS_STATUS', 'TIME_PERIOD', 'OBS_VALUE']
    columns = ['NAME'] + obs_keys

    def process_obs(elem, name):
        dct = elem.attrib
        # print(dct)
        data.append([name] + [dct[key] for key in obs_keys])

    def process_series(elem):
        dct = elem.attrib
        # print(dct)
        context = ET.iterwalk(
            elem, events=('end', ),
            tag='{http://www.federalreserve.gov/structure/compact/common}Obs'
            )
        fast_iter(context, process_obs, dct['SERIES_NAME'])

    def process_dataset(elem):
        nsmap = elem.nsmap
        # print(nsmap)
        context = ET.iterwalk(
            elem, events=('end', ),
            tag='{{{prefix}}}Series'.format(prefix=elem.nsmap['kf'])
            )
        fast_iter(context, process_series)

    with open(XMLpath, 'rb') as f:
        context = ET.iterparse(
            f, events=('end', ),
            tag='{http://www.federalreserve.gov/structure/compact/common}DataSet'
            )
        fast_iter(context, process_dataset)
        df = pd.DataFrame(data, columns=columns)

    ## Clean up directory
    filelist = [i for i in os.listdir(datapath) if i[-3:] in ['xml', 'xsd']]
    for i in filelist:
        os.remove(os.path.join(datapath, i))

    # Set missing values
    df.loc[(df['OBS_STATUS'].isin(['ND', 'NC', 'NA'])),'OBS_VALUE'] = np.nan

    return df

#----------------
# Get H.15 data
#----------------
data_download("H.15 Interest Rates",
              "https://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&filetype=zip",
              datapath,
              "H15.zip",
              headers=[('User-Agent', 'AnApp/1.0')])

## Process XML file
df = DDP_XML("H15.zip", "H15_data.xml", datapath)

## get unique list of series names
series_names = df['NAME'].unique()

## get frequency stubs from the end
stubs = list(set([stub[-2:] for stub in series_names]))

## cut dataset by frequency stubs and transpose
for i in stubs:
   freq_names = [name for name in series_names if name[-2:] == i]
   temp = df.loc[df['NAME'].isin(freq_names)]
   temp = temp.pivot(index='TIME_PERIOD', columns='NAME', values='OBS_VALUE')
   temp = temp.reset_index()
   clean_i=i.replace(".","")
   temp_name=os.path.join(datapath,"H15_{}.csv".format(clean_i))
   temp.to_csv(temp_name, index=False, na_rep=".")


## Close log
#------------------------------------------------------
print("Log closed on " + str(datetime.datetime.now()))
#------------------------------------------------------
sys.stdout = old_stdout
log_file.close()

