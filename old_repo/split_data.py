# import pandas as pd
# from sklearn.model_selection import train_test_split

# # Read the CSV file
# df = pd.read_csv('merge_data\merged_output_lazada.csv')

# # Split the data into train and test sets with a 4:1 ratio
# train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)

# # Save the train and test sets into separate CSV files
# train_df.to_csv('merge_data/train_lazada.csv', index=False)
# test_df.to_csv('merge_data/test_lazada.csv', index=False)

# print("Train and test files have been created.")


import pandas as pd
from sklearn.model_selection import train_test_split
import argparse

def split_data(source):
    # Determine the source file based on the argument
    if source == 'amazon'or source == 'lazada':
        source_file = f'merge_data/merged_output_{source}.csv'
        train_file = f'merge_data/train_{source}.csv'
        test_file = f'merge_data/test_{source}.csv'
    else:
        raise ValueError("Unsupported source. Please specify 'amazon' or 'lazada'.")

    # Read the CSV file
    df = pd.read_csv(source_file)

    # Split the data into train and test sets with a 4:1 ratio
    train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)

    # Save the train and test sets into separate CSV files
    train_df.to_csv(train_file, index=False)
    test_df.to_csv(test_file, index=False)

    print(f"Train and test files have been created for {source} data.")

if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser(description="Split data into train and test sets for Amazon or Lazada.")
    parser.add_argument('source', type=str, choices=['amazon', 'lazada'], help='Specify the data source: "amazon" or "lazada".')
    
    args = parser.parse_args()
    split_data(args.source)

