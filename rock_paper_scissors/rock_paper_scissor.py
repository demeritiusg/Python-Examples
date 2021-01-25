import random

while True:
    user_action = input('Enter a choice (rock, paper, scissors):')
    possible_actions = ['rock', 'paper', 'scissors']
    computer_acition = random.choice(possible_actions)
    print(f'\nYou chose {user_action}, computer chose {computer_acition}')

    if user_action == computer_acition:
        print(f'Both players selected {user_action}, it''s a tie!')
    elif user_action == 'rock':
        if computer_acition == 'scissors':
            print('rock smashes scissors! You win!')
        else:
            print('Paper covers rock! You lose.')
    elif user_action == 'paper':
        if computer_acition == 'rock':
            print('Paper covers rock!, you win!')
        else:
            print('Scissors cuts paper! you lose.')
    elif user_action == 'scissors':
        if computer_acition == 'paper':
            print('Scissors cuts paper! you win!')
        else:
            print('Rock smashes scissors. you lose.')
    
    play_again = input('Play again? (y/n): ')

    if play_again.lower() != 'y':
        break
    
