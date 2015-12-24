import os.path
import re

# run on VM with Spark initialized

def SparkWordCount(fileName, top_n):

    RDD = (sc
    			# read in textfile to RDD 
          .textFile(fileName, 8)
          # lower case, strip whitespace, remove unique characters
          .map(lambda x: re.sub(r'[^a-z0-9\s]', '', x.lower().strip()))
          # split by space and flatten
          .flatMap(lambda x: x.split(' '))
          # remove non-spaces
          .filter(lambda x: x != '')
          # map word with single value
          .map(lambda x: (x, 1))
          # sum counts
          .reduceByKey(lambda x, y: x + y)
          ) 
    
    # subset top number of desired words
    top = RDD.takeOrdered(num = top_n, key = lambda (k, v): -v)
    # print top words
    print '\n'.join(map(lambda (k, v): '{}: {}'.format(k, v), top))
    

SparkWordCount('shakespeare.txt', 5)
