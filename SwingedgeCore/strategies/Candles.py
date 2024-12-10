import DB

class Candles:
    
# returns all candles from db for a particular symbol and timeframe
# the functions queries the database for all candles rolled up to the 
# specific timeframe and for the given date range
    db_con
    
    # Initialize the database
    def __init__(self,db_creds):
        self.db_con = new DB(db_creds)
    
    def getCandles(symbol, tf, date_range, df=true):
        # construct the query
        # pass query to the DB class
        # DB¢lass knows database specifics and can return 
        # raw rows
        # convert the raw rows into dataframe and return to caller
        # if df parameter is set to false, return the response as a 
        # json object rather than a dataframe
        sql = "some sql"
        return db_con.getData(sql)
        