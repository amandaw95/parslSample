from parsl import load, python_app
from parsl.configs.local_threads import config
load(config)

import pandas as pd
import numpy as np

def splitDataframe(startColIndex, endColIndex, dFrame):
	df = pd.DataFrame()
	df = dFrame.iloc[: , np.r_[startColIndex : endColIndex]]
	return df


dfOriginal = pd.read_csv("/home/amanda/FYP/testcsv/test.csv")

numOfCols = dfOriginal.shape[1]
print(numOfCols)

from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

maxThreads = 4
local_threads = Config(
    executors=[
        ThreadPoolExecutor(
            max_threads=maxThreads,
            label='local_threads'
        )
    ]
)


lasThreadCols = 0

dfNew = pd.DataFrame()
splitDataFrames = list()

if (numOfCols % maxThreads == 0):
	eachThreadCols = numOfCols / maxThreads 
	for i in range (maxThreads):
		splitDataFrames.append((splitDataframe(i, i+eachThreadCols, dfOriginal)))
		
else:
	eachThreadCols = numOfCols // (maxThreads-1)
	lasThreadCols = numOfCols % maxThreads
	for i in range (0,(maxThreads-1)*eachThreadCols, eachThreadCols):
		print ("i", i)
		print("i+eachThreadCols", (i+eachThreadCols))
		splitDataFrames.append(splitDataframe(i,(i+eachThreadCols),dfOriginal))
		

	print("last thread",(eachThreadCols * (maxThreads-1))	)
	splitDataFrames.append(splitDataframe(eachThreadCols * (maxThreads-1), numOfCols, dfOriginal))


#print(splitDataFrames)


@python_app
def something(Dframe):
	print(Dframe)
	return "done"

results = []
for i in splitDataFrames:
	app_future = something(i)
	results.append(app_future)


# wait for all apps to complete
[r.result() for r in results]

	
