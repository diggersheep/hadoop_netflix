from tempfile import TemporaryFile
from itertools import islice
import os
import heapq
import re
import time
import io
import csv
import json
import sys

def load_bar ( progress: int, total: int, size: int ) -> str:
	s = '['
	fill = '■'
	void = ' '

	n = int((progress / total) * size)
	s += fill*n
	s += void*(size-n)

	s += ' ]'
	return s

def puts(msg):
	sys.stdout.write(msg)
	sys.stdout.flush()


class KaggleDecoder:

	def __init__(self, filename: str, headers: list, flush: int) -> None:
		self.headers = headers
		self.filename = filename
		self.block_limit = []
		self.line_buffer_size = 1024 * 4
		self.last_line = 0
		self.basename = os.path.basename(filename)
		self.flush = flush

		self._pos = 0
		self.fd = None
		self.csv = CSV_writer(self.flush)


	def determine_block(self, timer=True):
		with open(self.filename, 'r') as f:
			t = time.time()
			pattern = re.compile(r'[1-9][0-9]*:$')
			progress = 0
			i = 0
			while True:
				lines = list(islice(f, self.line_buffer_size))

				for line in lines:
					if pattern.match(line) is not None:
						self.block_limit.append(i)
					i += 1

				if not lines:
					break

			self.last_line = i+1
			tt = time.time() - t
			if timer:
				print('time determine block: {:03.04f}s'.format(tt))		

	def __del__(self):
		if self.fd is not None:
			self.fd.close()

	def csv_header(self):
		header = ''
		for i in range(len(self.headers)):
			if i != 0:
				header += ','
			header += self.headers[i]
		return (header + '\n') if len(header) > 0 else ''

	def get_csv_data(self, lines):
		s = self.csv_header()
		s += '\n'.join(lines)
		raw_data = csv.DictReader(io.StringIO(s), delimiter=',', quotechar='"')
		data = []
		for e in raw_data:
			data.append(e)
		return data

  # get CSV data + id of movie
	def __next__(self):
		if self._pos >= len(self.block_limit):
			raise StopIteration

		beg = self.block_limit[self._pos]
		end = 0
		if self._pos >= len(self.block_limit) - 1:
			end = self.last_line
		else:
			end = self.block_limit[self._pos+1]

		if self.fd is None:
			self.fd = open(self.filename, 'r')

		n_lines = end - beg
		lines = list(islice(self.fd, n_lines))

		data = {
			'id': lines[0][:-2],
			'data': self.get_csv_data(lines[1:])
		}

		#lines = list(islice(f, self.line_buffer_size))
		self._pos += 1
		return data

	def __iter__(self):
		self._pos = 0
		if self.fd is not None:
			self.fd.close()
			self.fd = None
		return self

	def generate_csv_lines(self, data):
		movie_id = data['id']
		lines = []
		for e in data['data']:
			line = []
			line.append(movie_id)
			for _, v in e.items():
				line.append(v)
			lines.append(','.join(line))
		return lines

	def push(self):
		print('Write CSV:')
		self.csv.open(  '../data/' + '.'.join(self.basename.split('.')[:-1]) + '.csv' )

		one = True
		ttt = time.time()
		t  = ttt
		tt = 0
		i = 0
		n_lines = 0
		total_proceed = 0
		for e in self:
		
			lines = self.generate_csv_lines(e)
			for line in lines:
				self.csv.write(line)
			n_lines += len(lines)
			total_proceed += len(lines)

			tt = time.time()
			puts(100 * '\r')
			
			if (i % self.flush) == 0:
				rs = n_lines / ( tt - t ) / 1000.0
				puts('{} ({: 5}/{}) -> {: 6.00f}kl/s (total: {: 2.03f}Ml) - time {: 4.03f}s'.format(
					load_bar(i+1, len(self.block_limit), 20),
					i+1,
					len(self.block_limit),
					rs,
					(total_proceed / 1000000.0),
					(time.time() - ttt) )
				)
				t = tt
				n_lines = 0
	
			i += 1

		print()
		print('total time processing: {: 4.02f}s'.format(time.time() - ttt) )
		print('mean average kilo-lines per second: {: 6.03f}kl/s'.format( total_proceed / (time.time() - ttt) / 1000 ))


class CSV_writer:
	def __init__(self, buffer_size : int = 1000):
		self.buffer_size = buffer_size
		
		if self.buffer_size < 1000:
			self.buffer_size = 1000

		self.buffer = []
		self.file = None

	def write (self, line):
		self.buffer.append(line)

		if len(self.buffer) >= self.buffer_size:
			self.flush()

	def flush(self):
		lines = '\n'.join(self.buffer)
		self.file.write(lines)
		self.file.write('\n')
		del self.buffer
		self.buffer = []

	def open(self, filename):
		if self.file is not None:
			self.close()
			self.file = None

		self.file = open(filename, 'w')

	def close(self):
		if self.file is not None:
			self.flush()
			self.file.flush()
			self.file.close()

	def __del__(self):
		self.close()
		del self.file
		del self.buffer


