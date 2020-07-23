# import necessary libraries
import config
import numpy as np
from mysql.connector import Error
import mysql.connector
import plotly.express as px
import time
from datetime import datetime as dt
import datetime
import pandas as pd
import os
from flask import Flask, render_template, request, redirect

# create instance of Flask app
app = Flask(__name__)


def create_chart_df(frame):
    times = []
    for k, v in frame.iterrows():
        time = pd.date_range(v.start_time, v.end_time, freq="60min",).time
        times.append([v.boat_length, v.provider, v.date, v.dockage, time])

    df = pd.DataFrame(
        times, columns=['boat_length', 'provider', 'date', 'dockage', 'hour'])
    df2 = df.explode(column='hour')
    df2['hour'] = df2.hour.apply(lambda x: x.strftime('%H:%M %p'))
    df2.reset_index(inplace=True)
    df3 = pd.DataFrame(df2.groupby(['provider', 'hour', 'date'])[
                       'boat_length'].sum())
    df3['dockage'] = df2.dockage.iloc[1]
    df3['availability'] = df3.dockage - df3.boat_length
    return df3
    

# List of dictionaries
dogs = [{"name": "Fido", "type": "Lab"},
        {"name": "Rex", "type": "Collie"},
        {"name": "Suzzy", "type": "Terrier"},
        {"name": "Tomato", "type": "Retriever"}]


# create route that renders index.html template
# @app.route("/")
# def index():

#     return render_template("index.html", dogs=dogs)


@app.route("/", methods=["GET", "POST"])
def index():
    host = config.host
    database = config.database
    user = config.user
    password = config.password
    conn = mysql.connector.connect(
        host=host, database=database, user=user, password=password)
    appointments = "SELECT user_id, status, service, provider, user_email, date, slot, slot_end, order_id \
                    FROM wp_jet_appointments \
                    ORDER BY slot asc"

    ap_data = pd.read_sql_query(appointments, conn)
    ap_data.to_csv("assets/data/wp_jet_appointments.csv", index=False)
    ap = pd.read_csv("assets/data/wp_jet_appointments.csv")
    duration = pd.to_datetime((ap.slot_end - ap.slot), unit='s').dt.time
    ap["duration"] = duration

    # Using a unix epoch time
    ap["date"] = pd.to_datetime(ap["date"], unit='s')
    ap["slot"] = pd.to_datetime(ap["slot"], unit='s').dt.time
    ap["slot_end"] = pd.to_datetime(ap["slot_end"], unit='s').dt.time
    ap["slot"] = ap["slot"].apply(lambda x: x.strftime('%I:%M %p'))
    ap["slot_end"] = ap["slot_end"].apply(lambda x: x.strftime('%I:%M %p'))
    ap["provider"] = ap["provider"].replace(
        415, "City Winery at the Chicago Riverwalk")
    ap["provider"] = ap["provider"].replace(417, "Marina City")
    ap["provider"] = ap["provider"].replace(421, "Pizzeria Portofino")
    ap["service"] = ap["service"].replace(478, "Dockage Under 24' 1hr")
    ap["service"] = ap["service"].replace(476, "Dockage Under 24' 2hr")
    ap["service"] = ap["service"].replace(474, "Dockage Under 24' 3hr")
    ap["service"] = ap["service"].replace(472, "Dockage 25' to 34' 1hr")
    ap["service"] = ap["service"].replace(470, "Dockage 25' to 34' 2hr")
    ap["service"] = ap["service"].replace(468, "Dockage 25' to 34' 3hr")
    ap["service"] = ap["service"].replace(466, "Dockage 35' to 44' 1hr")
    ap["service"] = ap["service"].replace(464, "Dockage 35' to 44' 2hr")
    ap["service"] = ap["service"].replace(462, "Dockage 35' to 44' 3hr")
    ap["service"] = ap["service"].replace(460, "Dockage 45' to 54' 1hr")
    ap["service"] = ap["service"].replace(458, "Dockage 45' to 54' 2hr")
    ap["service"] = ap["service"].replace(456, "Dockage 45' to 54' 3hr")
    ap["service"] = ap["service"].replace(454, "Dockage 55' to 64' 1hr")
    ap["service"] = ap["service"].replace(452, "Dockage 55' to 64' 2hr")
    ap["service"] = ap["service"].replace(450, "Dockage 55' to 64' 3hr")
    ap["service"] = ap["service"].replace(448, "Dockage 65' to 124' 1hr")
    ap["service"] = ap["service"].replace(446, "Dockage 65' to 124' 2hr")
    ap["service"] = ap["service"].replace(444, "Dockage 65' to 124' 3hr")
    ap["service"] = ap["service"].replace(442, "Dockage 125' and more 1hr")
    ap["service"] = ap["service"].replace(440, "Dockage 125' and more 2hr")
    ap["service"] = ap["service"].replace(438, "Dockage 125' and more 3hr")
    user_query = "SELECT * \
            FROM wp_usermeta \
            WHERE meta_key = 'length' "

    usermeta_data = pd.read_sql_query(user_query, conn)
    usermeta_data.to_csv("assets/data/wp_usermeta.csv", index=False)
    user = pd.read_csv("assets/data/wp_usermeta.csv")
    ap_m = pd.merge(ap, user, how="inner", on="user_id")
    pro_doc = {
        "provider": ["City Winery at the Chicago Riverwalk", "Marina City", "Pizzeria Portofino"],
        "dockage": [210, 55, 120]
    }

    pro_doc = pd.DataFrame(pro_doc)
    dock_data = pd.merge(pro_doc, ap_m, how="inner", on="provider")
    dock_data = dock_data[["provider", "dockage", "date",
                        "slot", "slot_end", "duration", "meta_value"]]
    #############################################

    
   
    # print(my_date)
    # # my_date = datetime.datetime.strptime(date_input, "%m/%d/%Y").strftime("%Y-%m-%d")
    # if date_input is None:
    #     mydate = "2020-06-11"
    # else:
    #     mydate = datetime.datetime.strptime(
    #         str(date_input), "%m/%d/%Y").strftime("%Y-%m-%d")
    if request.method == "POST":
        req = request.form
        date = req.get("date")
    # my_date = datetime.datetime.strptime(date_input, "%m/%d/%Y").strftime("%Y-%m-%d")
        if date is None:
            mydate = "2020-06-11"
        else:
            # mydate = datetime.datetime.strptime(date, "%m/%d/%Y").strftime("%Y-%m-%d")
            mydate = date
    # mydate = "2020-06-11"
        dock_data = dock_data[dock_data.date == mydate]
        dock_data = dock_data.rename(
            columns={"slot": "start_time", "slot_end": "end_time", "meta_value": "boat_length"})

        try:
            df = create_chart_df(dock_data)
            df.to_csv('output.csv')
            
        except(IndexError):
            print('WARNING'*500)
            print(f'No Data for {date}')
        
        frame = pd.read_csv('output.csv')
    return render_template("index.html", myframe=frame.to_html(classes='male'))


@app.route("/scrape")
def scrape():
    # Redirect back to home page
    return redirect("/")

    # # html_output = frame.to_html()
    # dphtml = r'<link rel="stylesheet" type="text/css" media="screen" href="css-table.css" />' + '\n'
    # dphtml += frame.to_html()

    # with open('templates/datatable.html', 'w') as f:
    #     f.write(dphtml)
    #     f.close()
    #     pass
    # return render_template("datatable.html")

if __name__ == "__main__":
    app.run(debug=True)


