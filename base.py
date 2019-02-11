from functools import reduce
from random import randint

films = {}
a = 'abcdefghijklmnopqrstuvwxyz'
for e in range(10):
	films[str(e)] = {
		'name': (a[e] * 10),
		'year': randint(1997, 2005)
	}

l = []
for i in range(1000):
	e = randint(0, 9)
	l.append({
		'film': str(e),
		'date': (films[str(e)]['year'] + randint(0, 30))
	})

def f ( e ):
	return {
		'film': films[e['film']]['name'],
		'diff': e['date'] - films[e['film']]['year']
	}

m = [f(e) for e in l if e['year'] >= 2000 ]
r = reduce( lambda a,b: a +b['diff'], m, 0 ) / len(m)

print(r)
