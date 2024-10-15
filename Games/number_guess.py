import random

def guess_the_number():

	number = random.randint(1, 100)
	attempts = 0

	print('Welcom to guess the number!')
	print("I'm thinking of a number between 1 and 100")

	while True:
		guess = int(input('Take a guess: '))
		attempts += 1

		if guess < number:
			print('Too low!')

		elif guess > number:
			print('Too high!')

		else:
			print(f"Congrats! you guessed the number in {attempts} attempts!")
			break

if __main__ == 'main':
	guess_the_number()
