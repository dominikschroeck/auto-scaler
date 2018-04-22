import csv
import itertools
import glob
import time


def drop_nulls(list):
    return [x for x in list if x is not None]


TempFolderPath="results"
outCombinedStatTxtName = "step1/FinalResults"
tempCsvFiles = glob.glob(str(TempFolderPath) + '/*.csv')

j= 0
finished = False
while len(tempCsvFiles) > 0:
    openedFileList = []
    l = 0
    for i in range(0,1000):
        if (len(tempCsvFiles) > 0):
            l= l+1
            openedFileList = openedFileList + [open (tempCsvFiles[0],'r')]
            tempCsvFiles.pop(0)

    print("Processing " + str(l) + " files")
    readers = [csv.reader(fn) for fn in openedFileList]

    lists = []
    print("GOING THROUGH ALL LISTS")
    list_count = len(readers)
    finished_count = 0

    result = open(outCombinedStatTxtName + str(j) + ".csv", 'w',encoding='utf-8')
    writer = csv.writer(result, delimiter=',')

    print("ZIPPING")
    for row_chunks in zip(*readers):

        tempFormattedRow = list(itertools.chain.from_iterable(row_chunks))

        writer.writerow(tempFormattedRow)
    result.close()
    [fn.close() for fn in openedFileList]
    j=j+1


TempFolderPath="step1"
outCombinedStatTxtName = "FinalResults"
tempCsvFiles = glob.glob(str(TempFolderPath) + '/*.csv')

j= 0
finished = False
while len(tempCsvFiles) > 0:
    openedFileList = []
    l = 0
    for i in range(0,1000):
        if (len(tempCsvFiles) > 0):
            l= l+1
            openedFileList = openedFileList + [open (tempCsvFiles[0],'r')]
            tempCsvFiles.pop(0)

    print("Processing " + str(l) + " files")
    readers = [csv.reader(fn) for fn in openedFileList]

    lists = []
    print("GOING THROUGH ALL LISTS")
    list_count = len(readers)
    finished_count = 0

    result = open(outCombinedStatTxtName + str(j) + ".csv", 'w',encoding='utf-8')
    writer = csv.writer(result, delimiter=',')

    print("ZIPPING")
    for row_chunks in zip(*readers):

        tempFormattedRow = list(itertools.chain.from_iterable(row_chunks))

        writer.writerow(tempFormattedRow)
    result.close()
    [fn.close() for fn in openedFileList]
    j=j+1
