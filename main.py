from spimi import SPIMI
import argparse

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description='Builds index using SPIMI')

	parser.add_argument('--input', default='data/gutenberg')
	parser.add_argument('-o','--output', default='index.txt')
	parser.add_argument('--block_size', type=int, default=50000)
	parser.add_argument('--positional', action='store_true')

	args = parser.parse_args()
	
	SPIMI(args.input,args.output, args.block_size, args.positional)


