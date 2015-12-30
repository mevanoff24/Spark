from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt
import numpy as np
import sys

conf = SparkConf().setMaster('local').setAppName('FriendsByAge')
sc = SparkContext(conf = conf)

def parse_lines(line):
	fields = line.split(',')
	age = int(fields[2])
	num_friends = int(fields[3])
	return (age, num_friends)

lines = sc.textFile(sys.argv[1])
rdd = lines.map(parse_lines)
totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])

results = average_by_age.collect()

for result in results:
	print result

x, y = np.array(results).T 

plt.plot(x, y, 'o-')
plt.ylabel('Number of Friends')
plt.xlabel('Age')
plt.show()



