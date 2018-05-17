import sqlite3

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

    def __init__(self, database_name):
        self.conn = None
        self.csv_df = None
        self.query_result_df = None
        self.database_name = database_name

        if self.database_name:
            self.create_connection()
        else:
            print('Please provide db name')


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
        try:
            self.csv_df = pd.read_csv(file_name)
        except FileNotFoundError:
            print('File does not exists!')


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
        if not self.csv_df.empty:
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
        try:
            return pd.read_sql_query(query, self.conn)
        except pd.pandas.io.sql.DatabaseError:
            print('Execution failed. Database error')


    def close_connection(self):
        """
        Closes the connection if connection is open
        """
        if self.conn:
            self.conn.close()
