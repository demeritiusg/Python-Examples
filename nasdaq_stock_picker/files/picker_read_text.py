


class ReadFile:
    fs = '.\data\stock_list.txt'
    def __init__(self):
        pass
    

    def read_file(self):
        print(self.fs)
        with open(self.fs) as f:
            #lines = [line.rstrip() for line in f]
            for item in f:
                print(item)
