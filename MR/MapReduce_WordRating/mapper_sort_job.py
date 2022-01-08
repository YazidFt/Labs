%%writefile mapper_rating.py

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8') #convert to unicode

for line in sys.stdin:
    try:
        word, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
        
    print "%d\t%s" % (count, word)