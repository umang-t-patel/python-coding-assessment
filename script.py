import sqlite3

import sys

import pandas as pd


class Database:

    def __init__(self, database_name=None):
        self.conn = None
        self.csv_df = None
        self.query_result_df = None

        if database_name:
            self.create_connection(database_name)


    def create_connection(self, database_name):

        try:
            self.conn = sqlite3.connect(database_name)

        except sqlite3.Error:
            print('Error connecting to database')

    def read_csv_from_file(self, file_name):
        self.csv_df = pd.read_csv(file_name)


    def add_update_movie_metadata_table(self, table_name):
        self.csv_df.to_sql(table_name, self.conn, if_exists='replace')


    def get_query_result_to_df(self, query):
        return pd.read_sql_query(query, self.conn)


    def close_connection(self):
        if self.conn:
            self.conn.close()


class ProblemStatements:

    def __init__(self):
        self.db = None
        self.filter_revenue = {}
        self.create_db()
        self.get_all_movie_metadata()


    def get_all_movie_metadata(self):
        query_get_all_movie_metadata = 'select * from movie_metadata;'
        self.movies_df = self.db.get_query_result_to_df(query_get_all_movie_metadata)
        self.db.close_connection()


    def create_db(self):
        self.db = Database('movies.db')
        self.db.read_csv_from_file('movie_metadata.csv')
        self.db.add_update_movie_metadata_table('movie_metadata')


    def exit_program(self):       
        print('Exiting program.') 
        sys.exit()

    def get_top_10_geners_with_decreasing_profitability(self):
        self.filter_revenue = {}
        for i in range(self.movies_df.shape[0]):
            for genre in self.movies_df['genres'][i].split('|'):
                movies = self.movies_df.iloc[i]
                self.revenue_based_on_filter_key(movies, genre)
        return self.least_profitable_movies_by_filter('Genre')


    def get_top_10_actors_with_decreasing_profitability(self):
        self.filter_revenue = {}
        for i in range(self.movies_df.shape[0]):
            movies = self.movies_df.iloc[i]
            actor = self.movies_df['actor_1_name'][i]
            if actor:
                self.revenue_based_on_filter_key(movies, actor)
        return self.least_profitable_movies_by_filter('Actor')


    def get_top_10_director_with_decreasing_profitability(self):
        self.filter_revenue = {}
        for i in range(self.movies_df.shape[0]):
            movies = self.movies_df.iloc[i]
            director = self.movies_df['director_name'][i]
            if director:
                self.revenue_based_on_filter_key(movies, director)
        return self.least_profitable_movies_by_filter('Director')


    def get_top_10_actor_director_pair_based_on_highest_IMDB_ratings(self):
        self.filter_revenue = {}
        for i in range(self.movies_df.shape[0]):
            movies = self.movies_df.iloc[i]
            director = self.movies_df['director_name'][i]
            actor = self.movies_df['actor_1_name'][i]
            if actor and director:
                self.rating_based_on_filter_key(movies, actor, director)
        return self.most_rated_actor_director_pair_movies()


    def rating_based_on_filter_key(self, movies, actor, director):
        filter_key = ''
        if str(movies['imdb_score']) != 'nan':
            filter_key = actor + '||||' + director
            imdb_score = float(movies['imdb_score'])
            if filter_key in self.filter_revenue:
                self.filter_revenue[filter_key] += imdb_score
            else:
                self.filter_revenue[filter_key] = {}
                self.filter_revenue[filter_key] = imdb_score


    def most_rated_actor_director_pair_movies(self):
        most_rated_movie = pd.DataFrame(list(self.filter_revenue.items()), columns=['Actor||||Director', 'Total IMDB Rating'])
        most_rated_movie = most_rated_movie.sort_values(by='Total IMDB Rating', ascending=False,)
        print('Top 10 actor director pair with most IMDB rating.\n')
        print(most_rated_movie.head(10))
        print()


    def revenue_based_on_filter_key(self, movies, filter_key):
        if str(movies['gross']) != 'nan' and str(movies['budget']) != 'nan':
            gross = int(movies['gross'])
            budget = int(movies['budget'])
            revenue = gross - budget
            if filter_key in self.filter_revenue:
                self.filter_revenue[filter_key] += revenue
            else:
                self.filter_revenue[filter_key] = {}
                self.filter_revenue[filter_key] = revenue


    def least_profitable_movies_by_filter(self, filter_key):
        most_profitable_genre = pd.DataFrame(list(self.filter_revenue.items()), columns=[filter_key, 'Revenue'])
        most_profitable_genre = most_profitable_genre.sort_values(by='Revenue')
        print('Top 10 ' + filter_key + ' with decreasing profitability.\n')
        print(most_profitable_genre.head(10))
        print()


def main():
    print('Initializing...')
    ps = ProblemStatements()
    problem_statement_switch = {
        0: ps.exit_program,
        1: ps.get_top_10_geners_with_decreasing_profitability,
        2: ps.get_top_10_actors_with_decreasing_profitability,
        3: ps.get_top_10_director_with_decreasing_profitability,
        4: ps.get_top_10_actor_director_pair_based_on_highest_IMDB_ratings
    }
    while True:
        argument = input('\nSelect Input: \n 1. Get top 10 geners with decreasing profitability. (It is based on "actor_1_name column") \n 2. Get top 10 actors with decreasing profitability. \n 3. Get top 10 directors with decreasing profitability. \n 4. Get top 10 actor director pair with most IMDB rating. \n 0. To exit \n\n')
        exec_ps = None
        if argument:
            exec_ps = problem_statement_switch.get(int(argument))
        if exec_ps:
            exec_ps()
        else:
            print('Invalid Selection.')


if __name__ == '__main__':
    main()

			