from collections import defaultdict

class DocumentEntry(list):
	def _process(self, seq):
		"""Makes iterable unique and sorted"""
		seen = set()
		seen_add = seen.add
		return sorted([ x for x in seq if not (x in seen or seen_add(x))])

	def __init__(self,iterable=[]):
		iterable = self._process(iterable)
		super(DocumentEntry,self).__init__(iterable)

	@property
	def count(self):
		return len(self)

	def __repr__(self):
		return str(self)

	def __str__(self):
		return '{}, {}'.format(self.count, list(self))


class PositionalIndexEntry(object):

	def __init__(self,*args,**kwargs):
		self.count = 0
		self.documents = defaultdict(DocumentEntry)

	def append(self,k,v):
		self.count += 1
		self.documents[k].append(v)

	def __repr__(self):
		return str(self)

	def __str__(self):
		return '{}, {}'.format(self.count, self.documents.items())

	def __add__(self, other):
		c = PositionalIndexEntry()
		for k in self.documents.keys() + other.documents.keys():
			if k in other.documents and k in self.documents:
				c.documents[k] = DocumentEntry(self.documents[k] + other.documents[k])
			elif k in self.documents:
				c.documents[k] = DocumentEntry(self.documents[k])
			else:
				c.documents[k] = DocumentEntry(other.documents[k])

		for k in c.documents.keys():
			c.count += c.documents[k].count
		return c



if __name__ == '__main__':
	print 'Index construction:'
	s = [('yellow', 1, 2), ('blue', 2, 1), ('yellow', 1, 3), ('blue', 2, 4), ('red', 1, 2), ('yellow', 2, 1)]

	d = defaultdict(PositionalIndexEntry)
	for k, v, p in s:
		d[k].append(v,p)

	print d.items()

	import sys
	print sys.getsizeof(d)
	print 

	from pickle import dumps,loads
	print 'Pickling test:'
	e = loads(dumps(d))
	print e.items()
	# total_frequency, [(doc_id, doc_freq, positions)]
	print e['yellow'], type(e['yellow'])

	print
	print 'Summation'
	d1 = d
	s2 = [('yellow', 1, 2), ('yellow', 1, 19), ('yellow', 5, 7),('yellow', 5, 3)]
	d2 = defaultdict(PositionalIndexEntry)
	for k, v, p in s2:
		d2[k].append(v,p)

	print d1['yellow'], '+', d2['yellow']
	print (d1['yellow']+d2['yellow'])