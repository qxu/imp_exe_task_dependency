import argparse
import time
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--slow', action='store_true')
parser.add_argument('--crash', action='store_true')
parser.add_argument('--hang', action='store_true')
args = parser.parse_args()

if args.crash:
    sys.exit(1)

if args.hang:
    time.sleep(60)
    sys.exit(1)

with open('file1.txt', 'w') as f:
    for i in range(10):
        if args.slow:
            time.sleep(0.5)
        f.write('{}\n'.format(time.time()))
