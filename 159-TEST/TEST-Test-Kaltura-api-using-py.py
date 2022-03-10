#@huy 
#feb 25, 2022
#v.0.1

from asyncio.windows_events import NULL
from KalturaClient import *
from KalturaClient.Plugins.Core import *
from KalturaClient.Plugins.Metadata import *
from pprint import pprint
import time,datetime,logging
import secret

#setting log file
logging.basicConfig(filename="kaltura-update-logs",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

#kaltura session
partner_id = secret.partner_id
partner_admin_secret = secret.partner_admin_secret
user_id=""
config = KalturaConfiguration(partner_id)
config.serviceUrl = "https://admin.video.ubc.ca/"
client = KalturaClient(config)
ks = client.session.start(
      partner_admin_secret,
      user_id,
      KalturaSessionType.ADMIN,
      partner_id)
client.setKs(ks)

#Filter subset of entries
filter = KalturaMediaEntryFilter()
filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_ASC #sort ascending
#filter.userIdEqual = "nguyenh" #filter by user
#filter.idEqual ="0_1efr20tb"
#set list to be empty
result = None



#Getting Custom Metatdata
#metaDataDC= KalturaMetadataProfile()
#metaDataDC.metadataObjectType= KalturaMetadataObjectType.ENTRY

#filterEntry= KalturaMetadataFilter()
#filterEntry.metadataProfileIdEqual =788
#filterEntry.objectIdEqual = "0_1ny5byxr"
#metaDataPlugIn= KalturaMetadataClientPlugin.get(client)


'''
for obj in resultForMetaData.objects:
    pprint(vars(obj))
'''
#set global intiial page =1    
numPage= 1
count=0

#get data per page
def getDatabyPage():
    pager = KalturaFilterPager()
    pager.pageSize = 100 #max is 500
    pager.pageIndex = numPage
    result = client.media.list(filter, pager)
    return result

#####function to add tags#####
def updateEntries(entry_id,curentTags):
    updateTag=KalturaMediaEntry()
    updateTag.tags = curentTags + ",migrate"
    client.media.update(entry_id,updateTag)

######function to add OCreationDate and OLastPlayedDate#####******
def updateOCDandOLPD(entry_id,OCreationDate,OLastPlayedDate):
    metadata_profile_id = 788
    xmlData = '<metadata><OCreationDate>'+str(OCreationDate)+'</OCreationDate><OLastPlayedDate>'+str(OLastPlayedDate)+'</OLastPlayedDate></metadata>'
    metadataObjectType= KalturaMetadataObjectType.ENTRY

    filterEntry= KalturaMetadataFilter()
    #filterEntry.metadataProfileIdEqual =788
    filterEntry.objectIdEqual = entry_id
    #check to see if meta data exists:
    metaData= client.metadata.metadata.list(filterEntry).objects
    checked= False
    for obj in metaData:
        pprint(vars(obj))
        if(obj.metadataProfileId == metadata_profile_id):
            checked= True
            metadata_profile_id = obj.id
    
    if (checked == True):
        client.metadata.metadata.update(metadata_profile_id, xmlData)
    else:
        client.metadata.metadata.add(metadata_profile_id,metadataObjectType,entry_id,xmlData)

#updateOCDandOLPD("0_1efr20tb",1588316400,1588316400)




#process data:
def doDataProcess(result):
    global count
    for obj in result.objects:
        try:
            if obj.createdAt >= 1588316400 or obj.lastPlayedAt >= 1588316400:
            #if  "migrate" in obj.tags:
                count += 1 #counting entries that are matched with criteria
                #print(obj.id + " by " + obj.userId + " " +obj.tags)
                print(obj.id + " has tags: "+ obj.tags)
                #print(datetime.datetime.fromtimestamp(obj.createdAt).strftime('%Y-%m-%d %H:%M:%S'))
                #print(datetime.datetime.fromtimestamp(obj.lastPlayedAt).strftime('%Y-%m-%d %H:%M:%S'))
                #pprint(vars(obj))
                #updateEntries(obj.id,obj.tags) #add tags
                #logging.info(obj.id + " has been assigned with a tag")
                #adding creation and lastplayed to custom fields
                #updateOCDandOLPD(obj.id,obj.createdAt,obj.lastPlayedAt)
                #logging.info(obj.id + " has been assigned with its orginal and last played dates")
        except:
            ####Add 0 for entries DONT HAVE LASTPLAYED DATE
            #updateOCDandOLPD(obj.id,obj.createdAt,0)
            #logging.info(obj.id + " has been assigned with its orginal and NO last played date")

            print("error")
            #logging.error(obj.id + " has errors")


#action: execuate each list 
while(numPage <5):
    result = getDatabyPage()
    doDataProcess(result)
    #sleep for 15mins (900)
    time.sleep(10)
    numPage = numPage + 1

print("final count is :" +str(count))




#Filter by me only
'''
filter.userIdEqual = "nguyenh"
filter.idEqual ="0_dptb9hvu"
filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_DESC
'''
'''
pager = KalturaFilterPager()
numPage= 1
pager.pageSize = 10
pager.pageIndex = numPage

'''


'''
while(numPage <4):
    #execute batches
    for obj in result.objects:
        #pprint(vars(obj))
        print(obj.creatorId)
        print(obj.id)
        count=count+1

    print(count)
    #sleep for 15mins (900 secs)
    time.sleep(15)
    #Execute next batch
    numPage = numPage + 1
    print(numPage)
    #Re-generate the list with new page

    result = client.media.list(filter, pager)

#0_2aqwtlj5


#updateEntries(obj.id)
#updateEntries(entry_id,updateTag)

'''
