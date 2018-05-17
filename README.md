# Python Coding Assessment

## Assessment Requirement:
    The following is an IMDB movie dataset available via Kaggle https://www.kaggle.com/carolzhangdc/imdb-5000-movie-dataset/data.
 
    The column names in this dataset should be more or less self-descriptive. Some of the key columns that pertain to this mini-project are: ‘actor_1_name’, ‘actor_2_name’, ‘actor_3_name’, ‘director_name’, ‘budget’ and ‘gross’. You can make assumptions about these names as you see fit in case they do not seem descriptive enough.
    
    The task requires the following:
        1. Import the file into a local db
        2. Then write functions in python to perform the following computations on this dataset. (Feel free to use pandas for this purpose):
            a. Compute the top 10 genres in decreasing order by their profitability. Note: You could compute profitability as simply as:
                i. ‘gross’ - ’budget’ or
                ii. (‘gross’ - ‘budget’)/’budget’
                iii. Anything advanced that you can think of
            b. Return the top 10 actors or directors in decreasing order by their profitability (use any definition you choose for profitability using the above guidance).
            c. Bonus questions (Note: If you choose to do any of the bonus questions below, any one question is more than adequate):
                Choice 1: Find the best actor, director pairs (up to 10) that have the highest IMDB_ratings, if there are indeed any such pairs.
                Choice 2: Any interesting questions that you would like to work on if you would  (for e.g. imdb_score, actor facebook_likes
                Choice 3: Build a REST API to return an actor’s information (simple text output)
        3. Write tests for your functions.
        4. Commit code to a git repo (gitlab or github) and send us a link to it.
        5. Also document your steps, libraries used and any instructions.

## Download and run app on local machine

### Clone the app on local machine

1. Clone the repository
 
        git clone https://github.com/umang-t-patel/python-coding-assessment.git
        
2. Go to the directory
    
        cd python-coding-assessment
        
### Run script on local machine
1. To run script on your local machine, you'll need a Python(3.6) set up, including Python, pip, and virtualenv
2. Create an isolated Python environment, and install pandas
    
    For windows,
 
        virtualenv env
        env\scripts\activate
        pip install -r requirements.txt
    
    For linux,
 
        virtualenv env
        source env/bin/activate
        pip install -r requirements.txt

3. Run script:

        python script.py
