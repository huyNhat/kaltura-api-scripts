import pandas as pd

FILE_PATH ='_assignment-entries-in-PROD-1.csv'
chunk_size = 500 #500
batch_no = 1

for chunk in pd.read_csv(FILE_PATH, chunksize=chunk_size):
    chunk.to_csv('files/_assignment' + str(batch_no) + '.csv', index=False)
    batch_no += 1
