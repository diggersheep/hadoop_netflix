from sys import argv
from kaggle_decoder import KaggleDecoder, load_bar
from time import sleep

if __name__ == '__main__':

	dirname = '../pre_data/'
	files = {
		'qualifying.txt': {
			'header': ['user_id','date'],
			'flush': 100
		},
		'combined_data_1.txt': {
			'header': ['user_id', 'rating', 'date'],
			'flush': 1
		},
		'combined_data_2.txt': {
			'header': ['user_id', 'rating', 'date'],
			'flush': 1
		},
		'combined_data_3.txt': {
			'header': ['user_id', 'rating', 'date'],
			'flush': 1
		},
		'combined_data_4.txt': {
			'header': ['user_id', 'rating', 'date'],
			'flush': 1
		},
	}

	for filename in files:
		headers = files[filename]['header']
		flush = files[filename]['flush']
		f = dirname + filename

		print((' ' + f + ' ').center(70, '='))
		print()
		print('determination of number of movie ids...')
		k = KaggleDecoder(f, headers, flush)
		k.determine_block()
		k.push()

		print()
		print()