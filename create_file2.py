import time

with open('file1.txt') as f1:
    lines = f1.readlines()

with open('file2.txt', 'w') as f2:
    for index, line in enumerate(lines):
        f2.write('{}, {}'.format(index, line))
