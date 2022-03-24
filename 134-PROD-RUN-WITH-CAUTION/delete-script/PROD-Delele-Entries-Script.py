#@huy 
#feb 25, 2022
#v.0.1.3

from KalturaClient import *
from KalturaClient.Plugins.Core import *
from KalturaClient.Plugins.Metadata import *
from pprint import pprint
import time,logging,math,os,glob
import secret.secretProd as key
import pandas as pd

#Setting log file
logging.basicConfig(filename="../logs/prod-delete-entries-logs-smoke-test",
                            filemode='a',
                            format='%(asctime)s,%(message)s',
                            datefmt='%d-%b-%y,%H:%M:%S',
                            level=logging.INFO)

#Establish Kaltura session
partner_id = key.partner_id
partner_admin_secret = key.partner_admin_secret
user_id= ""
config = KalturaConfiguration(partner_id)
config.serviceUrl = key.serviceUrl
client = KalturaClient(config) 

#Can extend the expiry lentgh here by specifying expiry higher than 24 hours
ks = client.session.start(
      partner_admin_secret,
      user_id,
      KalturaSessionType.ADMIN,
      partner_id)
client.setKs(ks)

# use glob to get all the csv files in the folder
# First go to the "files" folder
os.chdir("files" )
path = os.getcwd()
csv_files = glob.glob(os.path.join(path, "*.csv"))
#counter
count = 0

#Delete Entry
def deleteEntries(entryId):
    global count
    try:
        result = client.media.delete(entryId)
        count += 1
        logging.info(entryId + ' has been deleted')
    except Exception as Argument:
        logging.error(entryId +","+ str(Argument))

def processCSV():
    for f in csv_files:
        global count
        print("Processing: "+ f)
        df = pd.read_csv(f)
        for index, row in df.iterrows():
            print(row['entryId'])
            deleteEntries(row['entryId'])
        #sleep for 10 secs
        time.sleep(10)

def main():
    processCSV()
    logging.info("num of deleted entries: " + str(count))

if __name__ == "__main__":
    main()

