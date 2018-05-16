import sqlite3

import sys

import pandas as pd


class Database:
    """
    Database class will create database connection. Gets data from CSV and DB. Stores data in DB

    Parameters
    ----------
    database_name (string) : Pass database name. Default to None.

    Examples
    -------
    movies.db
    """

    def __init__(self, database_name=None):
        self.conn = None
        self.csv_df = None
        self.query_result_df = None
        self.database_name = database_name

        if self.database_name:
            self.create_connection()


    def create_connection(self):
        """
        Creates connection with the given database name

        """

        try:
            self.conn = sqlite3.connect(self.database_name)

        except sqlite3.Error:
            print('Error connecting to database')

    def read_csv_from_file(self, file_name):
        """
        Pandas read_csv function is used to read csv files and store it into a dataframe

        Parameters
        ----------
        file_name (string) : CSV file name.

        Examples
        -------
        movie_metadata.csv

        """
        self.csv_df = pd.read_csv(file_name)


    def add_update_movie_metadata_table(self, table_name):
        """
        Pandas to_sql function is used to store dataframe into sql table

        Parameters
        ----------
        table_name (string) : SQL table name.

        Examples
        -------
        movie_metadata

        """
        self.csv_df.to_sql(table_name, self.conn, if_exists='replace')


    def get_query_result_to_df(self, query):
        """
        Pandas read_sql_query function is used to store the query output in pandas dataframe

        Parameters
        ----------
        query (string) : Query for execution.
        
        Examples
        -------
        select * from movie_metadata;

        Returns
        -------
        dataframe : pandas dataframe with query output

        """
        return pd.read_sql_query(query, self.conn)


    def close_connection(self):
        """
        Closes the connection if connection is open
        """
        if self.conn:
            self.conn.close()


class ProblemStatements:
    """
    ProblemStatements class contains all the function which will get results of all tasks.
    Also, it contains some helper function.
    1. get_top_10_geners_with_decreasing_profitability = Get top 10 geners with decreasing profitability. 
    2. get_top_10_actors_with_decreasing_profitability = Get top 10 actors with decreasing profitability. (It is based on "actor_1_name column")
    3. get_top_10_director_with_decreasing_profitability = Get top 10 directors with decreasing profitability.
    4. get_top_10_actor_director_pair_based_on_highest_IMDB_ratings = Get top 10 actor director pair with most IMDB rating.
    5. exit_program = To exit the program

    """

    def __init__(self):
        self.db = None
        self.filter_revenue = {}
        self.create_db()
        self.get_all_movie_metadata()


    def create_db(self):
        """
        Create database connection to movies database.
        Read data from CSV file and store it in to movie database with table name movie_metadata.

        """
        self.db = Database('movies.db')
        self.db.read_csv_from_file('movie_metadata.csv')
        self.db.add_update_movie_metadata_table('movie_metadata')


    def get_all_movie_metadata(self):
        """
        Get the output of the sql query to get all records from movie data all at once.
        Assign output of sql query to movies_df dataframe for later use.
        Close database connection after retrieving data. 

        """
        query_get_all_movie_metadata = 'select * from movie_metadata;'
        self.movies_df = self.db.get_query_result_to_df(query_get_all_movie_metadata)
        self.db.close_connection()


    def exit_program(self): 
        """
        Print texiting message and Exit the program.

        """      
        print('Exiting program.') 
        sys.exit()


    def get_top_10_geners_with_decreasing_profitability(self):
        """
        Get top 10 geners with decreasing profitability.
        Profitability = Gross amount - Budget amount

        Returns
        -------
        dataframe : pandas dataframe with top 10 geners with decreasing order of movie profitabitlity

        """  
        self.filter_revenue = {}
        for item in range(self.movies_df.shape[0]):
            for genre in self.movies_df['genres'][item].split('|'):
                movies = self.movies_df.iloc[item]
                self.revenue_based_on_filter_key(movies, genre)
        return self.least_profitable_movies_by_filter('Genre')


    def get_top_10_actors_with_decreasing_profitability(self):
        """
        Get top 10 actors with decreasing profitability.
        actors_1_name is the coulmn referred for actor name.

        Returns
        -------
        dataframe : pandas dataframe with top 10 actor with decreasing order of movie profitabitlity

        """  
        self.filter_revenue = {}
        for item in range(self.movies_df.shape[0]):
            movies = self.movies_df.iloc[item]
            actor = self.movies_df['actor_1_name'][item]
            if actor:
                self.revenue_based_on_filter_key(movies, actor)
        return self.least_profitable_movies_by_filter('Actor')


    def get_top_10_director_with_decreasing_profitability(self):
        """
        Get top 10 director with decreasing profitability.

        Returns
        -------
        dataframe : pandas dataframe with top 10 director with decreasing order of movie profitabitlity

        """  
        self.filter_revenue = {}
        for item in range(self.movies_df.shape[0]):
            movies = self.movies_df.iloc[item]
            director = self.movies_df['director_name'][item]
            if director:
                self.revenue_based_on_filter_key(movies, director)
        return self.least_profitable_movies_by_filter('Director')


    def get_top_10_actor_director_pair_based_on_highest_IMDB_ratings(self):
        """
        Get top 10 actor director pair based on highest IMDB ratings.
        Pair is contactenated with ||||. 
        For eg. Johnny Depp||||Gore Verbinski
                where, Johnny Depp is actor and Gore Verbinski is director

        Returns
        -------
        dataframe : pandas dataframe with top 10 actor director pair with total IMDB rating

        """  
        self.filter_revenue = {}
        for item in range(self.movies_df.shape[0]):
            movies = self.movies_df.iloc[item]
            director = self.movies_df['director_name'][item]
            actor = self.movies_df['actor_1_name'][item]
            if actor and director:
                self.rating_based_on_filter_key(movies, actor, director)
        return self.most_rated_actor_director_pair_movies()


    def rating_based_on_filter_key(self, movies, actor, director):
        """This functions creates a dictionary with actor director pairs as key and value as total imdb_score.

        Parameters
        ----------
        movies (string) : Series of a dataframe
        actor (string) : actor name.
        director (string) : director name.

        """
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
        """
        This functions creates a dataframe from filter_revenue dictionary. Then sorts the dataframe by Total IMDB Rating in decreasing order.
        And prints top 10 records of the dataframe
        
        """
        most_rated_movie = pd.DataFrame(list(self.filter_revenue.items()), columns=['Actor||||Director', 'Total IMDB Rating'])
        most_rated_movie = most_rated_movie.sort_values(by='Total IMDB Rating', ascending=False,)
        print('Top 10 actor director pair with most IMDB rating.\n')
        print(most_rated_movie.head(10))
        print()


    def revenue_based_on_filter_key(self, movies, filter_key):
        """
        This functions creates a dictionary with filter_key key and value as total imdb_score.
        filter_key can be either geners, actor name, director name.


        Parameters
        ----------
        movies (string) : Series of a dataframe
        filter_key (string) : It can be geners, actor name or director name.
        
        """
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
        """
        This functions creates a dataframe from filter_revenue dictionary. Then sorts the dataframe by Revenue
        And prints top 10 records of the dataframe.

        Parameters
        ----------
        filter_key (string) : It can be geners, actor name or director name.
        
        """
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
        argument = input('\nSelect Input: \n 1. Get top 10 geners with decreasing profitability. \n 2. Get top 10 actors with decreasing profitability. (It is based on "actor_1_name column") \n 3. Get top 10 directors with decreasing profitability. \n 4. Get top 10 actor director pair with most IMDB rating. \n 0. To exit \n\n')
        exec_ps = None
        if argument:
            exec_ps = problem_statement_switch.get(int(argument))
        if exec_ps:
            exec_ps()
        else:
            print('Invalid Selection.')


if __name__ == '__main__':
    main()

			