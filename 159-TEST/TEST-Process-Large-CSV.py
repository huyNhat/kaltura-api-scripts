import pandas as pd
import logging

FILE_PATH ='sample-data-1.csv'

#Adding chunksize here: https://www.chrislettieri.com/the-best-way-to-read-a-large-csv-file-in-python/#:~:text=%20Four%20ways%20to%20read%20a%20large%20CSV,but%20has%20the%20best%20performance%2C%20assuming...%20More%20

#Setting log file
logging.basicConfig(filename="logs/testing-chunksize",
                            filemode='a',
                            format='%(asctime)s,%(message)s',
                            datefmt='%d-%b-%y,%H:%M:%S',
                            level=logging.INFO)

def processCSV():
    df = pd.read_csv(FILE_PATH)
    for index, row in df.iterrows():
        try:
            #deleteEntries(row['entryID'])
            print("entryID#: "+ row['entryID']+ 'has been deleted')
            logging.info(row)

            #logging.info("entryID#: "+ row['entryID']+ 'has been deleted')
        except Exception as Argument:
            print(row['entryID,']+ str(Argument))
            #logging.error(row['entryID']+ str(Argument))


def process_data():
    chunksize = 500
    total_sum = 0
    column_index = 1
    with pd.read_csv(FILE_PATH, chunksize=chunksize) as reader:
        for chunk in reader.iterrows():
            #total_sum += sum_column(chunk, column_index)
            print("hello +:"+chunk)
            logging.info(chunk)


if __name__ == "__main__":
    processCSV()
