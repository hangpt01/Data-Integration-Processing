import json

with open('Dataset/IMDB/sample.json', 'r') as file:
    data = json.load(file)

with open('Dataset/IMDB/formatted_sample.json', 'w') as file:
    json.dump(data, file, indent=4)
