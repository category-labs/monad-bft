import json
import sys

file_name = sys.argv[1]

with open(file_name, 'r') as f:
    data = json.load(f)

    if 'leader' not in data:
        sys.exit(0)

    data = data['leader']
    data = {int(k): v for (k, v) in data.items()}
    rounds = sorted(data.keys())

    freq = {}

    for Round in rounds:
        author = data[Round]
        if author not in freq:
            freq[author] = 1
        else:
            freq[author] += 1

    print(file_name, '\n')
    for author in sorted(freq.keys()):
        print(author, '->', float(freq[author]) * 100.0 / float(len(rounds)))
    print('====================')
