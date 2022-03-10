#@huy 
#feb 25, 2022
#v.0.1.1

from KalturaClient import *
from KalturaClient.Plugins.Core import *
from KalturaClient.Plugins.Metadata import *
from pprint import pprint
import time,datetime,logging,math
import secretTest

#Setting log file
logging.basicConfig(filename="test-clone-assignment-log-1",
                            filemode='a',
                            format='%(asctime)s,%(message)s',
                            datefmt='%d-%b-%y,%H:%M:%S',
                            level=logging.INFO)

                            #%(name)s %(levelname)s



#Establish Kaltura session
partner_id = secretTest.partner_id
partner_admin_secret = secretTest.partner_admin_secret
user_id=""
config = KalturaConfiguration(partner_id)
config.serviceUrl = secretTest.serviceUrl
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
#filter.userIdEqual = "nguyenh" #multiple users->userIdIn
#filter.idEqual ="0_1efr20tb"

#Set list to be empty
result = None

#Set global intial page =1 and count=0   
numPage= 1
count=0
pageSize= 500 #max is 500
numOfPage = 0
lastProcessedCreatedAt=0 #keeping track of last CreatedAt


def getTotalOfPages():
    totalCount= client.media.count(filter)
    return math.ceil(totalCount/pageSize) #round up

#Get data per page
def getDatabyPage():
    pager = KalturaFilterPager()
    pager.pageSize = pageSize
    pager.pageIndex = numPage
    result = client.media.list(filter,pager)
    return result

#Add a tag
def updateEntries(entry_id,curentTags):
    updateTag=KalturaMediaEntry()
    #updateTag.tags = curentTags + ",keep2022"
    #client.media.update(entry_id,updateTag)
    #logging.info(str(entry_id) + "'s tag has been updated with " + curentTags + ",keep2022")

#Process data:
def doDataProcess(result):
    global count
    for obj in result.objects:
        try:
            if "_assignment" in obj.userId:
                if(obj.lastPlayedAt is not None and obj.plays is not None):
                    print(str(obj.id) + ","+ str(obj.userId)+"," + str(obj.createdAt) +"," +str(obj.lastPlayedAt)+ ","+str(obj.plays))
                    logging.info(str(obj.id) + ","+ str(obj.userId)+"," + str(obj.createdAt) +"," +str(obj.lastPlayedAt)+ ","+str(obj.plays)) 
                else:
                    print(str(obj.id) + ","+ str(obj.userId)+"," + str(obj.createdAt) +",null, null")
                    logging.info(str(obj.id) + ","+ str(obj.userId)+"," + str(obj.createdAt) +",null, null")
                count += 1

        except Exception as Argument:
            logging.error(str(Argument))

def main():
    #Execute each list 
    #print("im here")
    #print(getTotalOfPages())
#
    global numPage
    while(numPage <= getTotalOfPages()):#156
        result = getDatabyPage()
        doDataProcess(result)
        #sleep for 5 seconds
        time.sleep(5)
        numPage = numPage + 1
    logging.info("Final count is :" +str(count))
    print("Final count is :" +str(count))

if __name__ == "__main__":
    main()





