#@huy 
#feb 25, 2022
#v.0.1.2

from KalturaClient import *
from KalturaClient.Plugins.Core import *
from KalturaClient.Plugins.Metadata import *
from pprint import pprint
import time,datetime,logging,math
import secret.secretTest as key

#Setting log file
logging.basicConfig(filename="kaltura-logs-1",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%d-%b-%y %H:%M:%S',
                            level=logging.INFO)

#Parameters
customTagName = "keep2022"
cutoffDate = 1588316400


#Establish Kaltura session
partner_id = key.partner_id
partner_admin_secret = key.partner_admin_secret
user_id=""
config = KalturaConfiguration(partner_id)
config.serviceUrl = "https://admin.video.ubc.ca/"
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
#filter.userIdIn = "nguyenh"
#filter.idEqual ="0_1efr20tb"

#Set list to be empty
result = None

#Set global intial page =1 and count=0   
numPage= 1
pageSize= 500 #assume max is 5 instead of 500 with a max entries retrieved is 100
totalEntriesProcess=0
totalNumOfSubsetEntries=0
maxCount = 10000 # assume max is 100 instead of 10000 |max entries can be retrieved at a time
loopCount= 0   
lastProcessedCreatedAt=0 #keeping track of last CreatedAt

def getTotalCount():
    totalCount= client.media.count(filter)
    return totalCount


def getTotalOfPages():
    totalCount= getTotalCount()
    return math.ceil(totalCount/pageSize) #round up

#Get data per page
def getDatabyPage():
    pager = KalturaFilterPager()
    pager.pageSize = 100 #max is 500
    pager.pageIndex = numPage
    result = client.media.list(filter,pager)
    return result

#Add a tag
def updateEntries(entry_id,curentTags):
    updateTag=KalturaMediaEntry()
    updateTag.tags = curentTags + ","+ customTagName
    client.media.update(entry_id,updateTag)
    logging.info(str(entry_id) + "'s tag has been updated with " + curentTags + ","+customTagName)

#Process data:
def doDataProcess(result):
    global count
    for obj in result.objects:
        try:
            if (obj.createdAt >= cutoffDate or (obj.lastPlayedAt is not None and obj.lastPlayedAt >= cutoffDate)) and customTagName not in obj.tags:
                updateEntries(obj.id,obj.tags)
                count += 1

        except Exception as Argument:
            logging.error(str(Argument))
            
def main():
    global numPage, filter, lastProcessedCreatedAt, loopCount

    numOfOuterLoop = math.ceil((getTotalCount()/maxCount)) #round up
    print("numOfOuterLoop is:" +str(numOfOuterLoop))

    while(loopCount <= numOfOuterLoop):
        #Recheck totalCount to only process the next 10K of entries
        print("current total count is: "+str(getTotalCount()))
        numOfInnerLoop =  math.ceil(getTotalCount()/pageSize)
        if (numOfInnerLoop >= 20): #20 is max loop for page size of 500
            numOfInnerLoop = 20
        else:
            numOfInnerLoop = numOfInnerLoop

        while(numPage <= numOfInnerLoop): # 5 x 20 = 100 entries
            result = getDatabyPage()
            doDataProcess(result)
            #sleep for 5 seconds
            time.sleep(5)
            numPage += 1
        print("lastProcessedCreatedAt is: "+ str(lastProcessedCreatedAt))
        #Re-generate the new list starting from the lastProcessedCreatedAt
        try:
            filter.createdAtGreaterThanOrEqual = lastProcessedCreatedAt
        except Exception as Argument:
            logging.error(str(Argument))
        #Reset the paging
        numPage = 1        
        #increase the count for outter loop
        loopCount += 1
        print("current loop count is: "+str(loopCount))
    
    #print("lastProcessedCreatedAt is: "+ str(lastProcessedCreatedAt))
    #logging.info("Final count is :" +str(count))
    logging.info("Num of entries processed: " +str(totalEntriesProcess))
    logging.info("Num of subset entries processed: " +str(totalNumOfSubsetEntries))
    print("Final count is :" +str(totalEntriesProcess))
    

if __name__ == "__main__":
    main()
