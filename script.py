import ProblemStatements as pbs


def main():
    print('Initializing...')
    
    ps = pbs.ProblemStatements()

    problem_statement_switch = {
        0: ps.exit_program,
        1: ps.get_top_10_geners_with_decreasing_profitability,
        2: ps.get_top_10_actors_with_decreasing_profitability,
        3: ps.get_top_10_director_with_decreasing_profitability,
        4: ps.get_top_10_actor_director_pair_based_on_highest_IMDB_ratings
    }

    while True:
        argument = input('\nSelect Input: \n 1. Get top 10 geners with decreasing profitability. \n 2. Get top 10 actors with decreasing profitability. (It is based on "actor_1_name column") \n 3. Get top 10 directors with decreasing profitability. \n 4. Get top 10 actor director pair with most IMDB rating. \n 0. To exit \n\n')
        exec_ps = None
        if argument.isdigit():
            exec_ps = problem_statement_switch.get(int(argument))
        if exec_ps:
            exec_ps()
        else:
            print('Invalid Selection.')


if __name__ == '__main__':
    main()