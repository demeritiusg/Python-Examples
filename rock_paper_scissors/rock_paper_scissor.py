import random
from enum import IntEnum

class Action(IntEnum):
    Rock = 0
    Paper = 1
    Scissors = 2

def user_pick():
    user_input = input("enter a choice (rock[0], paper[1], scissors[2]):")
    selection = int(user_input)
    action = Action(selection)
    return action

def computer_pick():
    selection = random.randint(0, len(Action) - 1)
    action = Action(selection)
    return action

def play_game(user_selection, computer_selection):
    li

    if user_selection == computer_selection:
        print(f'both players selected {user_selection.name}')
    elif user_selection == Action.Rock:
        if computer_selection == Action.Scissors:
            print('rock smashes scissors! You win!')
        else:
            print('Paper covers rock! You lose.')
    elif user_selection == Action.Paper:
        if computer_selection == Action.Rock:
            print('Paper covers rock!, you win!')
        else:
            print('Scissors cuts paper! you lose.')
    elif user_selection == Action.Scissors:
        if computer_selection == Action.Paper:
            print('Scissors cuts paper! you win!')
        else:
            print('Rock smashes scissors. you lose.')
    
    
while True:
    try:
        user_selection = user_pick()
    except ValueError as e:
        select_range = f'[0, {len(Action) - 1}]'    
        print(f'range out of bounds, 0 - {select_range}')
        continue

    computer_selection = computer_pick()
    play_again(user_selection, computer_selection)

    play_again = input('Play again? (y/n): ')
    if play_again.lower() != 'y':
        break
    
