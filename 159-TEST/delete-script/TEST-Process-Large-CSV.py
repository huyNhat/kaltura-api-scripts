import pandas as pd
import logging,os,glob,time

#Setting log file
logging.basicConfig(filename="../logs/test-delete-entries-logs-2",
                            filemode='a',
                            format='%(asctime)s,%(message)s',
                            datefmt='%d-%b-%y,%H:%M:%S',
                            level=logging.INFO)

# use glob to get all the csv files in the folder
# First go to the "files" folder
os.chdir("files" )
path = os.getcwd()
csv_files = glob.glob(os.path.join(path, "*.csv"))


def processCSV():
    for f in csv_files:
        print("Processing: "+ f)
        df = pd.read_csv(f)
        for index, row in df.iterrows():
            try:
                #deleteEntries(row['entryID'])
                print("entryID#: "+ row['entryID']+ 'has been deleted')
                logging.info(row['entryID']+ ' has been deleted')
            except Exception as Argument:
                logging.error(row['entryID']+ str(Argument))
        #sleep for 10 secs
        time.sleep(10)

if __name__ == "__main__":
    processCSV()