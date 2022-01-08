import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

# The input data from File: A U B

for line in sys.stdin:
	key, count = unicode(line.strip()).split(',')
	#day_count = int(day_count)
	keys = key.split(' ')
	
	if(len(keys) > 1):
		value = keys[0] + " " + count
		print("%s\t%s" % (keys[1], value))
	else:
		print("%s\t%s" % (keys[0], count))
	
	
	
	
	
