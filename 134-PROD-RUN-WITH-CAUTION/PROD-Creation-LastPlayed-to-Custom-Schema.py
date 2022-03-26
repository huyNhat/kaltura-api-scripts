#@huy 
#feb 25, 2022
#v.0.1.3

from asyncio.windows_events import NULL
from KalturaClient import *
from KalturaClient.Plugins.Core import *
from KalturaClient.Plugins.Metadata import *
from pprint import pprint
import time,datetime,logging,math
import secret.secretProd as key

#Setting log file
logging.basicConfig(filename="logs/prod-custom-schema-logs-2",
                            filemode='a',
                            format='%(asctime)s,%(message)s',
                            datefmt='%d-%b-%y,%H:%M:%S',
                            level=logging.INFO)

#Establish Kaltura session
partner_id = key.partner_id
partner_admin_secret = key.partner_admin_secret
user_id=""
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

#session = ""
#pprint(client.session.get(session).expiry)

#Filter subset of entries
filter = KalturaMediaEntryFilter()
filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_ASC #sort ascending
#filter.createdAtGreaterThanOrEqual = 1514793600 #before the first video in KMC
filter.userIdEqual = "nguyenh" #multiple users->userIdIn
filter.idEqual ="0_9ntue2if"

#Set list to be empty
result = None

#Set global intial page =1 and count=0   
numPage= 1
pageSize= 500 #assume max is 5 instead of 500 with a max entries retrieved is 100
totalEntriesProcess=0
totalNumOfSubsetEntries=0
maxCount = 10000 # assume max is 100 instead of 10000 | max entries can be retrieved at a time
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
    pager.pageSize = pageSize
    pager.pageIndex = numPage
    result = client.media.list(filter,pager)
    return result

#Add OCreationDate and OLastPlayedDate
def updateOCDandOLPD(entry_id,OCreationDate,OLastPlayedDate, OPlays):

    print(entry_id+","+OCreationDate+","+OLastPlayedDate+","+OPlays)
    metadata_pid = key.metadata_profile_id
    '''
    xmlData =""
    if(OLastPlayedDate != NULL and OPlays != NULL):
        xmlData='<metadata><OCreationDate>'+str(OCreationDate)+'</OCreationDate><OLastPlayedDate>'+str(OLastPlayedDate)+'</OLastPlayedDate><OPlays>'+str(OPlays) +'</OPlays></metadata>'
    elif(OLastPlayedDate == NULL and OPlays >=0):
        xmlData='<metadata><OCreationDate>'+str(OCreationDate)+'</OCreationDate><OPlays>'+str(OPlays) +'</OPlays></metadata>'
    else:
        xmlData='<metadata><OCreationDate>'+str(OCreationDate)+'</OCreationDate></metadata>'
    '''

    xmlData ='<metadata><OCreationDate>1616428511</OCreationDate><OLastPlayedDate>1643666400</OLastPlayedDate><OPlays>9</OPlays></metadata>'
    print(xmlData)

    metadataObjectType= KalturaMetadataObjectType.ENTRY
    filterEntry= KalturaMetadataFilter()
    filterEntry.objectIdEqual = entry_id
    #check to see if meta data exists:
    metaData= client.metadata.metadata.list(filterEntry)
    checked= False
    for obj in metaData.objects:
        #pprint(vars(obj))
        if(obj.metadataProfileId == metadata_pid):
            checked= True
            metadata_pid = obj.id
    try:
        if(checked == True):
            result=client.metadata.metadata.update(metadata_pid,xmlData)
            pprint(vars(result))
            logging.info(entry_id+ " has been updated with a new custom schema")
        else:
            result=client.metadata.metadata.add(metadata_pid,metadataObjectType,entry_id,xmlData)
            pprint(vars(result))            
            logging.info(entry_id + " has been assigned a custom schema") 
    except Exception as Argument:
        logging.error(str(entry_id)+","+str(Argument))
    

#For Testing purposes:
def getMetaData():
    metadata_pid = key.metadata_profile_id
    metadataObjectType= KalturaMetadataObjectType.ENTRY
    filterEntry= KalturaMetadataFilter()
    filterEntry.objectIdEqual = "0_9ntue2if"
    metaData= client.metadata.metadata.list(filterEntry)
    for obj in metaData.objects:
        pprint(vars(obj))
 
#Process data:
def doDataProcess(result):
    global totalEntriesProcess,lastProcessedCreatedAt,totalNumOfSubsetEntries
    for obj in result.objects:
        try:
            #totalNumOfSubsetEntries += 1
            if(obj.lastPlayedAt is not None and obj.plays is not None):
                print(str(obj.id) + ","+ str(obj.userId)+"," + str(obj.createdAt) +"," +str(obj.lastPlayedAt)+ ","+str(obj.plays))
                updateOCDandOLPD(obj.id,obj.createdAt,obj.lastPlayedAt,obj.plays) 
            else:
                ##########Store OPlays if not Null BUT IT COULD BE ZERO
                if(obj.plays is not None):########
                    print(str(obj.id) + ","+ str(obj.userId)+"," + str(obj.createdAt) +",null"+ ","+str(obj.plays))
                    updateOCDandOLPD(obj.id,obj.createdAt,NULL,obj.plays)
                else:
                    print(str(obj.id) + ","+ str(obj.userId)+"," + str(obj.createdAt) +",null, null")
                    updateOCDandOLPD(obj.id,obj.createdAt,NULL,NULL)
                #updateOCDandOLPD(obj.id,obj.createdAt,NULL)
                #logging.info(str(obj.id) + ","+ str(obj.userId)+"," + str(obj.createdAt) +",null, null")
            totalEntriesProcess += 1
            #keep track of the last process entry's createdAt
            lastProcessedCreatedAt = obj.createdAt
 
        except Exception as Argument:
            logging.error(str(obj.id)+","+str(Argument))

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
    #logging.info("Num of subset entries processed: " +str(totalNumOfSubsetEntries))
    print("Num of entries processed :" +str(totalEntriesProcess))
    

if __name__ == "__main__":
    #main()
    #print(str(getTotalCount()))
    getMetaData()
    #updateOCDandOLPD('0_9ntue2if','1616428511','1643666400','9')