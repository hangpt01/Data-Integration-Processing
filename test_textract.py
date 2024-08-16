import textract
text = textract.process("Dataset/IMDB/testing_data.json")
print(text)