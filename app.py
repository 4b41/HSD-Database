import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime
import argparse
import sqlalchemy

def get_datetime():
    return datetime.now().strftime("%Y-%m-%d")

def parse_settings(): 
    # TODO: add more settings and parse like a json thing by splitting ": " or just use json, maybe sort settings before
    with open("settings.txt","r") as f:
        settings = f.read().split("\n")
        settings = [setting for setting in settings if setting != ""]
    if (len(settings) != 3):
        settings = []
        settings.append(datetime.fromtimestamp(0).strftime("%Y-%m-%d"))
        settings.append(str(get_datetime()))
        settings.append("1d")
        with open('settings.txt',"w") as f:
            f.write("\n".join(settings))
    return settings

def data_query():
    engine = sqlalchemy.create_engine('sqlite:///database')
    settings = parse_settings();
    with open("symbols.txt","r") as f:
        symbols = f.read().split("\n")
        symbols = [symbol for symbol in symbols if symbol != ""]
    if(len(symbols) == 0):
        print("Empty query!")
    else:
        df = get_data(symbols, settings[0], settings[1], settings[2])
        to_sql(df,symbols,settings[2],engine)
        with open("symbols.txt", "w") as f: # empties query list
            f.write("")

def update_query(args):
    with open("symbols.txt","r") as f:
        symbols = f.read().split("\n")
        symbols = [symbol for symbol in symbols if symbol != ""]
    toappend = list(args.add_tickers.split(","))
    toappend = [s for s in toappend if s not in symbols]
    if (len(toappend) > 0):
        symbols.extend(toappend)
        symbols = list(set(symbols))
        symbols.sort()
        with open("symbols.txt", "w") as f:
            f.write("\n".join(symbols))
        print("Symbols: {} have been added to the query!".format(toappend))
    else:
        print("No new symbols have been added!")


def remove_query(args):
    with open("symbols.txt","r") as f:
        symbols = f.read().split("\n")
        symbols = [symbol for symbol in symbols if symbol != ""]
    toremove= list(args.add_tickers.split(","))
    toremove = [s for s in toremove if s in symbols]
    for s in toremove:
        symbols.remove(s)
    with open("symbols.txt", "w") as f:
        f.write("\n".join(symbols))

def clear_query(): 
    with open("symbols.txt", "w") as f:
         f.write("")

def print_query():
    with open("symbols.txt","r") as f:
        symbols = f.read().split("\n")
        symbols = [symbol for symbol in symbols if symbol != ""]
    print(symbols)

def get_data(symbols, start_date, end_date, interval):
    data = []
    for symbol in symbols:
        data.append(yf.download(symbol,start=start_date,end=end_date,interval=interval).reset_index());
    return data;

def to_sql(frames, symbols, interval, engine):
    for frame,symbol in zip(frames,symbols):
        symbol = symbol.upper()
        filename = "{}_{}".format(symbol, interval);
        if sqlalchemy.inspect(engine).has_table(filename):
            print("Table {} already exists, updating table...".format(filename))
            stored = pd.read_sql(filename, engine)
            stored_prev = stored[stored.Date<frame["Date"].min()]
            stored_post = stored[stored.Date>frame["Date"].max()]
            frame = pd.concat([stored_prev,frame,stored_post])
            frame.to_sql(filename, engine, index=False, if_exists="replace")
        else:
            print("Table {} has been created!".format(filename))
            frame.to_sql(filename, engine, index=False)


def set_start_date(args):
    settings = parse_settings()
    settings[0] = args.set_start_date
    with open("settings.txt","w") as f:
        f.write("\n").join(settings)

def set_end_date(args):
    settings = parse_settings()
    settings[1] = args.set_end_date
    with open("settings.txt","w") as f:
        f.write("\n").join(settings)

def set_interval(args):
    settings = parse_settings()
    if args.set_interval in ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"]:
        interval_length = (datetime.strptime(settings[1], "%Y-%m-%d")-datetime.strptime(settings[0], "%Y-%m-%d"))
        if (args.set_interval == "1m" and interval_length.days <= 7):
            settings[2] = args.set_interval
        elif (args.set_interval == "1m"):
            settings[2] = "1d"
            print("{} data is only available for past 7 days!".format(args.set_interval))
        if (args.set_interval in ["2m", "5m", "15m", "30m", "60m", "90m", "1h"] and interval_length.days <= 60):
            settings[2] = args.set_interval
        elif (args.set_interval in ["2m", "5m", "15m", "30m", "60m", "90m", "1h"]):
            settings[2] = "1d"
            print("{} data is only available for past 60 days!".format(args.set_interval))
    else:
        print("Invalid interval, please choose from (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)")
    with open("settings.txt","w") as f:
        f.write("\n").join(settings)

def parser():
    #TODO: fix help and make these neat
    parser = argparse.ArgumentParser(description="um")
    parser.add_argument(
        "--submit_query",
        action='store_true',
        help="yo gobba",
    )
    parser.add_argument(
        "--add_tickers",
        default="",
        help="Seperate symbols with \",\"",
    )
    parser.add_argument(
        "--remove_tickers",
        default="",
        help="Seperate symbols with \",\"",
    )
    parser.add_argument(
        "--current_tickers",
        action='store_true',
        help="yo gobba",
    )
    parser.add_argument(
        "--clear_tickers",
        action='store_true',
        help="yo gobba",
    )
    parser.add_argument(
        "--set_start_date",
        default="",
        help="End date"
    )
    parser.add_argument(
        "--set_end_date",
        default="",
        help="End date"
    )
    parser.add_argument(
        "--set_interval",
        default="",
        help="End date"
    )
    parser.add_argument(
        "--get_csv",
        default="",
        help="yo gobba",
    )
    return parser.parse_args();

if __name__ == "__main__":
    args = parser()
    if(args.submit_query):
        data_query()
    elif(args.add_tickers != ""):
        update_query(args)
    elif(args.remove_tickers != ""):
        remove_query(args)
    elif(args.current_tickers):
        print_query()
    elif(args.clear_tickers):
        clear_query()
    elif(args.set_start_date != ""):
        set_start_date(args)
    elif(args.set_end_date != ""):
        set_end_date(args)
    elif(args.set_interval != ""):
        set_interval(args)