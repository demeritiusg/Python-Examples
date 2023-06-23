
class Person():
    multiplier = 69
    def __init__(self, name, number):
        if not isinstance(name, str) or not isinstance(number, int):
            raise ValueError('wrong values')
        self.name = name
        self.number = number

    def display(self):
        print(self.name)
        print(self.number)

    def details(self):
        print(f'the persons name is {self.name} and number is {self.number}')

    def some_math(self):
        x = self.multiplier*self.number
        return x

    
