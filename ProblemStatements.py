import sys

import pandas as pd

import Database as db


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
        self.db = db.Database('movies.db')
        if self.db:
            self.db.read_csv_from_file('movie_metadata.csv')
            self.db.add_update_movie_metadata_table('movie_metadata')
        else:
            self.exit_program()


    def get_all_movie_metadata(self):
        """
        Get the output of the sql query to get all records from movie data all at once.
        Assign output of sql query to movies_df dataframe for later use.
        Close database connection after retrieving data. 

        """
        query_get_all_movie_metadata = 'select * from movie_metadata;'
        self.movies_df = self.db.get_query_result_to_df(query_get_all_movie_metadata)
        self.db.close_connection()
        if self.movies_df.empty:
            self.exit_program()        


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
