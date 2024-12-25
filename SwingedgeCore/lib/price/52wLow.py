# Returns all symbols where the open of the first minute candle is greater than the close of last 
# one minute candle of previous day

# E.g. the 
# first minute candle on 24th is 4:00 am - its opening price is 40 (A)
# last minute candle on 23rd december is 8:59 pm - its closing price is 30 (B)

# This function will return all symbols where the A is greater than B by a min of x%
# The default value of x is 5