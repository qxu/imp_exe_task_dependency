import argparse
import time
import os

parser = argparse.ArgumentParser()
parser.add_argument('number')
args = parser.parse_args()

os.remove('file{}.txt'.format(number))
