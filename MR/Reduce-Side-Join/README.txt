The Mapper and Reducer files present MapReduce jobs to inner-join two datasets: A & B
Each line in Files A ,B is a <key, value> pair:

In File-A: <word, total_count>
able,991
about,11
burger,15
actor,22
...

File-B: <date word, day-count>
Jan-01 able,5
Feb-02 about,3
Mar-03 about,8
Apr-04 able,13
Feb-22 actor,3
Feb-23 burger,5
Mar-08 burger,2
Dec-15 able,100
...

Desired output: File-C <date word, day-count total_count>