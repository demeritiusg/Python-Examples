
file = 'D:\Git\Python-Examples\\nasdaq_picker\data\stock_list.txt'

def read_file(file):
    with open(file) as f:
        lines = [line.rstrip() for line in f]

    print(lines)
