import pandas as pd
from datetime import datetime as dt
from datetime import date, timedelta


LOCAL_DIR='#redacted#'

def main():
        
        df = pd.read_csv(LOCAL_DIR + 'data.csv')
        df.drop(['L1','L2','L3','L4','L5','L6','L7','L8','L9','L10','L11','L12','L13','L14','L15','L16','L17','L18','L19','L20','T1','T2','T3','T4','T5','T6','T7','T8','T9','T10','T11','T12','T13','T14','T15','T16','T17','T18','T19','T20'], axis=1, inplace=True)
        df.to_csv(LOCAL_DIR + 'processed.csv', index = False)
    
if __name__ == '__main__':

        main() 
