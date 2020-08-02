# import necessary libraries
import datetime
import os
import time
from argparse import RawTextHelpFormatter
from datetime import datetime as dt

import config
import mysql.connector
import numpy as np
import pandas as pd
import plotly.express as px
from flask import Flask, redirect, render_template, request
from mysql.connector import Error


# create instance of Flask app
app = Flask(__name__)


def create_chart_df(myframe):
    times = []
    for k, v in myframe.iterrows():
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
    




# create route that renders index.html template
# @app.route("/")
# def index():

#     return render_template("index.html", dogs=dogs)


@app.route("/", methods=["GET", "POST"])
def index():
    global df
    global theframe
    global myframe 

    host = config.host
    database = config.database
    user = config.user
    password = config.password
    conn = mysql.connector.connect(
        host=host, database=database, user=user, password=password)
    appointments = "SELECT \
					wp_jet_appointments.ID, \
					wp_jet_appointments.status, \
					a.post_title as service, \
					b.post_title as provider, \
					d.meta_value as dockage, \
					wp_jet_appointments.user_email, \
					wp_jet_appointments.date, \
					wp_jet_appointments.slot AS start_time, \
					wp_jet_appointments.slot_end AS end_time, \
					wp_jet_appointments.slot_end - wp_jet_appointments.slot as duration, \
					wp_jet_appointments.order_id AS confirmation, \
					u.display_name as captain, \
					c.meta_value AS boat_length, \
					wp_jet_appointments.phone, \
					wp_jet_appointments.comments \
				FROM wp_jet_appointments \
				INNER JOIN wp_posts AS a ON wp_jet_appointments.service = a.ID \
				INNER JOIN wp_posts AS b ON wp_jet_appointments.provider = b.ID \
				INNER JOIN wp_postmeta AS d ON wp_jet_appointments.provider = d.post_id \
				INNER JOIN wp_users AS u ON wp_jet_appointments.user_id = u.ID \
				INNER JOIN wp_usermeta AS c ON u.ID = c.user_id \
				WHERE order_id > 0 \
				AND d.meta_key = 'dock_length' \
				AND c.meta_key = 'length' \
				ORDER BY date, start_time ASC"

    ap_data = pd.read_sql_query(appointments, conn)
    ap_data.to_csv("assets/data/wp_jet_appointments.csv", index=False)
    ap = pd.read_csv("assets/data/wp_jet_appointments.csv")

    # Using a unix epoch time
    ap["date"] = pd.to_datetime(ap["date"], unit='s')
    ap["start_time"] = pd.to_datetime(ap["start_time"], unit='s').dt.time
    ap["end_time"] = pd.to_datetime(ap["end_time"], unit='s').dt.time
    ap["start_time"] = ap["start_time"].apply(lambda x: x.strftime('%I:%M %p'))
    ap["end_time"] = ap["end_time"].apply(lambda x: x.strftime('%I:%M %p'))

    dock_data = ap[["provider", "dockage", "date", "start_time", "end_time", "duration", "boat_length"]]
    #############################################

    myframe = pd.DataFrame()
   
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
            theframe = pd.read_csv('output.csv')
        
        theframe = pd.read_csv('output.csv')
        
    return render_template("index.html", myframe= (theframe.to_html(classes='male')))


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
