from datasets import load_dataset

ds = load_dataset('nyuuzyou/steambans')
ds['train'].to_csv('steambans.csv')
