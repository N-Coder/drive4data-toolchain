from os import listdir
from os.path import isfile, join


class SafeFileWalker:
    def __init__(self, root):
        self.stack = [root]

    def __iter__(self):
        return self

    def __next__(self):
        while len(self.stack) > 0:
            file = self.stack.pop()
            if isfile(file):
                return file
            else:
                self.stack.extend([join(file, sub) for sub in listdir(file)])
        raise StopIteration
