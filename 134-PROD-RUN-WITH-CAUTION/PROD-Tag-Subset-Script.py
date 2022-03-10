#@huy 
#feb 25, 2022
#v.0.1.1


from KalturaClient import *
from KalturaClient.Plugins.Core import *
from KalturaClient.Plugins.Metadata import *
from pprint import pprint
import time,datetime,logging
import secret

#Setting log file
logging.basicConfig(filename="kaltura-prod-tagging-logs-1",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%d-%b-%y %H:%M:%S',
                            level=logging.INFO)

#Establish Kaltura session
partner_id = secret.partner_id
partner_admin_secret = secret.partner_admin_secret
user_id=""
config = KalturaConfiguration(partner_id)
config.serviceUrl = secret.serviceUrl
client = KalturaClient(config) 

#Can extend the expiry lentgh here by specifying expiry higher than 24 hours
ks = client.session.start(
      partner_admin_secret,
      user_id,
      KalturaSessionType.ADMIN,
      partner_id)
client.setKs(ks)

#session = ""
#pprint(client.session.get(session).expiry)

#Filter subset of entries
filter = KalturaMediaEntryFilter()
filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_ASC #sort ascending
filter.userIdEqual = "nguyenh" #multiple users->userIdIn
#filter.idEqual ="0_1efr20tb"

#Set list to be empty
result = None

#Set global intial page =1 and count=0   
numPage= 1
count=0

#Get data per page
def getDatabyPage():
    pager = KalturaFilterPager()
    pager.pageSize = 500 #max is 500
    pager.pageIndex = numPage
    result = client.media.list(filter,pager)
    return result

#Add a tag
def updateEntries(entry_id,curentTags):
    updateTag=KalturaMediaEntry()
    updateTag.tags = curentTags + ",keep2022"
    #client.media.update(entry_id,updateTag)
    #logging.info(str(entry_id) + "'s tag has been updated with " + curentTags + ",keep2022")

#Process data:
def doDataProcess(result):
    global count
    for obj in result.objects:
        try:
            if (obj.createdAt >= 1588316400 or (obj.lastPlayedAt is not None and obj.lastPlayedAt >= 1588316400)) and "keep2022" not in obj.tags:
                logging.info(str(obj.id)) 
                count += 1

        except Exception as Argument:
            logging.error(str(Argument))

def main():
    #Execute each list 
    global numPage
    while(numPage <5):
        result = getDatabyPage()
        doDataProcess(result)
        #sleep for 15mins (900)
        time.sleep(5)
        numPage = numPage + 1
    logging.info("Final count is :" +str(count))
    print("Final count is :" +str(count))

if __name__ == "__main__":
    main()