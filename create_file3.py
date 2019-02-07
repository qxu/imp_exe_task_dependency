import time

with open('file2.txt') as f1:
    lines = f1.readlines()

with open('file3.txt', 'w') as f3:
    for line in lines:
        f3.write('{}{}'.format(line, line))
