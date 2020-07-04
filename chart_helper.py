def create_chart_df(frame):
    times = []
    for k,v in frame.iterrows():    
        time = pandas.date_range(v.start_time, v.end_time, freq="60min",).time
        times.append([v.boat_length,v.provider,v.date,v.dockage,time])

    df= pd.DataFrame(times,columns=['boat_length','provider','date','dockage','hour'])
    df2 = df.explode(column='hour')
    df2['hour'] = df2.hour.apply(lambda x: x.strftime('%H:%M %p'))
    df2.reset_index(inplace=True)
    df3 = pd.DataFrame(df2.groupby(['provider','hour'])['boat_length'].sum())
    df3['dockage'] = df2.dockage.iloc[1]
    df3['availability']= df3.dockage - df3.boat_length
    return df3
