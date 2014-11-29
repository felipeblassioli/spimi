from os import listdir
import re
from collections import defaultdict
from pickle import dump, load

BLOCK_NAME_FMT = 'block{}.dat'

def tokenize(text):
	# input: "A first-class ticket to the U.S.A. isn't expensive?"
	# output: ['A', 'first-class', 'ticket', 'to', 'the', 'U.S.A.', "isn't", 'expensive', '?']
	return re.findall("(?:[A-Z]\.)+|\w+(?:[-']\w+)*|[-.(]+|\S\w*", text)

def parse_document(doc_id, text):
	"""Parse the doc in a stream of term-docId pairs which we call tokens"""
	return [ (term, doc_id, pos) for pos, term in enumerate(tokenize(text)) ]

def generate_token_stream(data_dir='data/gutenberg'):
	# Generate token_stream
	token_stream = []
	print 'Documents Indexed:'
	for i, filename in enumerate(sorted(listdir(data_dir))):
		print '\t', i, filename
		with open(data_dir+'/'+filename) as f:
			token_stream.extend(parse_document(i,f.read()))

	# for x in token_stream:
	# 	print x
	print 'tokens_count', len(token_stream)
	return token_stream

from positional_index import PositionalIndexEntry
def SPIMI_Invert(block_id, token_stream):
	"""
	Page 73:

	SPIMI-Invert(token_stream)
	 1  output_file = NewFile()
	 2  dictionary = NewHash()
	 3  while (free memory available)
	 4  do token <- next(token_stream)
	 5    if term(token) not in dictionary
	 6      then postings_list = AddToDictionary(dictionary,term(token))
	 7      else postings_list = GetPostingsList(dictionary,term(token))
	 8    if full(postings_list)
	 9      then postings_list = DoublePostingsList(dictionary,term(token))
	10    AddToPostingsList(postings_list, docID(token))
	11  sorted_terms <- SortTerms(dictionary)
	12  WriteBlockToDisk(sorted_terms,dictionary,output_file)
	13  return output_file

	Each call of SPIMI-Invert writes a block to disk.
	The index of the block is its dictionary and the postings_list.
	"""

	output_file = BLOCK_NAME_FMT.format(block_id)

	index = Index()
	for tkn in token_stream:
		index.insert(tkn)

	index = index.sorted()

	with open(output_file, 'wb') as f:
		dump(index, f)

	with open(output_file.replace('.dat','.txt'), 'wt') as f:
		f.write('\n'.join(['%s, %s' % (t, str(index[t])) for t in index.keys()]))

	return output_file

def _make_blocks(token_stream, block_size):
	block_id = 0
	begin,end = 0,0
	tokens_count = len(token_stream)

	while end + block_size < tokens_count:
		end += block_size
		print 'block from ', begin, end
		yield SPIMI_Invert(block_id, token_stream[begin:end]) 
		block_id += 1
		begin = end
		
	print 'block from ', end, len(token_stream)
	yield SPIMI_Invert(block_id, token_stream[end:])

import operator
def merge_blocks(terms, blocks, output_file='index.txt'):
	with open(output_file, 'wt') as index:
		files = [ open(b,'rb') for b in blocks ]
		d = reduce(operator.add,[ load(f) for f in files ], Index()).items()
		for term, postings_list in sorted(d, key=lambda x: x[0]):
		 	index.write('%s, %s\n' % (term, str(postings_list)))
		for f in files: f.close()

	print 'generated', output_file


def SPIMI(input_dir='data/gutenberg', output_file='index.txt', block_size=50000, is_positional=False):
	if is_positional:
		from positional_index import PositionalIndex as Index
		globals()['Index'] = Index

	token_stream = generate_token_stream(input_dir)
	blocks = [ block for block in _make_blocks(token_stream, block_size) ]
	terms = [ token[0] for token in token_stream ]
	merge_blocks(terms, blocks, output_file)
