import sys

current_word = None
current_total = None

set_join_keys = []
set_join_values = []


for line in sys.stdin:
	word, value = line.strip().split(' ')
	values = value.split(' ')
	
	if(word != current_word):
		for i in range(len(set_join_keys)):
		    final_key = ser_join_keys[i]
			final_value = ser_join_values[i] + " " + current_total
			print("%s\t%s", (final_key, final_value))
		
		current_word = word
		set_join_keys = []
		ser_join_values = []
	
	
	if(len(values) > 1):
		new_key = values[0] + " " + word
		new_value = values[1]
		set_join_keys.add(new_key)
		ser_join_values.add(new_value)
	else:
		current_total = value


# The last join results		
for i in range(len(set_join_keys)):
	final_key = ser_join_keys[i]
	final_value = ser_join_values[i] + " " + current_total
	print("%s\t%s", (final_key, final_value))	
		
		
	