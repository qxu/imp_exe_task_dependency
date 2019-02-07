import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument('number')
args = parser.parse_args()

number = args.number

with open('file{}.txt'.format(number)) as f:
    lines = f.readlines()
    if lines:
        print('file{} first line:'.format(number), repr(lines[0]))
    print('file{} line len:'.format(number), len(lines))

time.sleep(1)
