# version 2.. showing growth

import random

choices = ['rock', 'paper', 'scissors']

def determine_winner(player, computer):
    if player == computer:
        return "It's a draw!"
    elif (player == 'rock' and computer == 'scissors') or \
         (player == 'scissors' and computer == 'paper') or \
         (player == 'paper' and computer == 'rock'):
        return "You win!"
    else:
        return "Computer wins!"
    
def play():
    while True:
        player_choice = input("Enter rock, paper, or scissors (or quit or stop playing):").lower()

        if player_choice == 'quit':
            print("Thanks for playing")
            break

        if player_choice not in choices:
            print("Invailed choice, try again")
            continue

        # computer randomly chooses
        computer_choice = random.choice(choices)
        print(f"Computer chose {computer_choice}")

        # determine winner and prints results
        result = determine_winner(player_choice, computer_choice)
        print(result)


# run game
play()