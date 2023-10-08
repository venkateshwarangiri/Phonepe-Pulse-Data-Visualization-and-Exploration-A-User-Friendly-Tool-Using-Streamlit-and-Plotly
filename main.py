import git
import os
import pandas as pd
import json
import matplotlib.pyplot as plt
import plotly.express as px
import requests
import mysql.connector as sql
import streamlit as st
import plotly.express as px
from streamlit_option_menu import option_menu
import PIL
from PIL import Image
import plotly.graph_objects as go


# connecting to the local database

my_db = sql.connect(
    host='localhost',
    user='root',
    password='root',
    database='phone_pe_pulse'
)

mycursor = my_db.cursor(buffered=True)

# setting the page configuration

icon = Image.open('pulse.png')
st.set_page_config(page_title="Phone pe pulse data visualization",
                   page_icon=icon,
                   layout="wide",
                   )
# designing the nav-bar

selected = option_menu(None,
                       options=["About", "Transactions-Insights",
                                "Users-Insights"],
                       icons=["house", "cash-coin", "bi-people"],
                       default_index=0,
                       orientation="horizontal",
                       styles={"container": {"width": "100%"},
                               "options": {"margin": "10px"},
                               "icon": {"color": "black", "font-size": "24px"},
                               "nav-link": {"font-size": "24px", "text-align": "center", "margin": "15px", "--hover-color": "#6F36AD"},
                               "nav-link-selected": {"background-color": "#6F36AD"}})

# functions to be used


# function for getting geojson file from the git hub link
def geo_state_list():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data = json.loads(response.content)
    geo_state = [i['properties']['ST_NM'] for i in data['features']]
    geo_state.sort(reverse=False)
    return geo_state

# function for getting the state list


def original_state_list():
    mycursor.execute("""select distinct state 
                            from aggregated_transaction
                            order by state asc;""")
    s = mycursor.fetchall()
    original_state = [i[0] for i in s]
    return original_state

# fuction for making a dictionary from states in geojson to our states


def state_dict():
    original = original_state_list()
    geo = geo_state_list()
    data = {}
    for i in range(0, len(original)):
        data[original[i]] = geo[i]
    return data

# taking the key value of that dictionary as it needs to be passed in the chloropeth map locations


def state_list_val(data):
    dat = []
    for key, val in data.items():
        dat.append(val)
    return dat

# function for getting original state list


def state_list():
    mycursor.execute(f"""select distinct state 
                            from aggregated_transaction
                            order by state asc;""")
    data = mycursor.fetchall()
    original_state = [i[0] for i in data]
    return original_state

# function for getting the year list


def year_list():
    mycursor.execute(
        "SELECT distinct year FROM phone_pe_pulse.aggregated_transaction order by year asc;")
    data = mycursor.fetchall()
    data = [i[0] for i in data]
    return data

# function for getting the quarter list


def quarter_list():
    mycursor.execute(
        "SELECT distinct quarter FROM phone_pe_pulse.aggregated_transaction order by quarter asc;")
    data = mycursor.fetchall()
    data = [i[0] for i in data]
    return data

# function for getting the transaction type


def get_transaction_type():
    mycursor.execute(
        "SELECT distinct transaction_type FROM phone_pe_pulse.aggregated_transaction;")
    data = mycursor.fetchall()
    data = [i[0] for i in data]
    return data


# function for getting average transaction value
def agg_trans_avg(agg_trans):
    data = []
    for i in range(0, len(agg_trans)):
        avg = agg_trans.iloc[i]["Transaction_amount"] / \
            agg_trans.iloc[i]["Transaction_count"]
        data.append(avg)
    return data

# getting transaction data


def get_map_transaction():
    mycursor.execute("SELECT * FROM phone_pe_pulse.map_transaction;")
    data = mycursor.fetchall()
    d = pd.DataFrame(data, columns=mycursor.column_names)
    return d

# creating index for tables


def new_frame(v):
    i = [i for i in range(1, len(v)+1)]
    data = pd.DataFrame(v.values, columns=v.columns, index=i)
    return data

# function for getting aggregrated users


def get_agg_users():
    mycursor.execute("SELECT * FROM phone_pe_pulse.aggregated_users;")
    data = mycursor.fetchall()
    d = pd.DataFrame(data, columns=mycursor.column_names)
    return d

# getting map users data


def get_map_users():
    mycursor.execute("SELECT * FROM phone_pe_pulse.map_users;")
    data = mycursor.fetchall()
    d = pd.DataFrame(data, columns=mycursor.column_names)
    return d

# finding average users


def users_trans_avg(agg_trans):
    data = []
    for i in range(0, len(agg_trans)):
        avg = agg_trans.iloc[i]["App_opens"] / \
            agg_trans.iloc[i]["Registered_user"]
        data.append(avg)
    return data


if selected == "About":

    im1 = Image.open("cover1.jpg")
    st.image(im1, width=1500)

    col1, col2 = st.columns(2)

    with col1:
        im = Image.open("info.jpg")
        st.markdown("")
        st.image(im)

    with col2:

        st.markdown("#### India with 89.5 million digital transactions in the year 2022 has topped the list of five countries in digital payments, according to data from MyGovIndia")
        st.markdown(
            "#### India is number one in digital payments. India is one of the countries where mobile data is the cheapest")
        st.markdown("#### The Reserve Bank of India’s ‘Payments Vision 2025’ document observes that ‘payment systems foster economic development and financial stability’ while supporting financial inclusion")
        st.markdown("#### The adoption of digital payment methods, while accelerated by the COVID-19 pandemic, has also been enabled by the widening number of banks which have backed the UPI system")

        st.markdown("#### to make United Payments Interface (UPI) more user-friendly,RBI has proposed incorporating Artificial Intelligence-powered conversational features on UPI, to enable digital payments through voice commands")

        im = Image.open("Phonepe_Pulse.jpg")
        st.image(im)

if selected == "Transactions-Insights":
    with st.container():
        # showing quarter wise financial type wise year wise transaction for states
        st.markdown("#### :black[TRANSACTIONS INSIGHTS]")
        col1, col2, col3 = st.columns(3)
        # select box
        with col1:
            state = st.selectbox(label="Select the state",
                                 options=state_list(), index=0)
        with col2:
            year = st.selectbox(label="Select the year",
                                options=year_list(), index=0)
        with col3:
            quarter = st.selectbox(
                label="Select the Quarter", options=quarter_list(), index=0)

        col1, col2 = st.columns(2)

        with col1:
            def get_aggregated_user():
                mycursor.execute(
                    "SELECT * FROM phone_pe_pulse.aggregated_transaction;")
                data = mycursor.fetchall()
                df = pd.DataFrame(data, columns=mycursor.column_names)
                return df
            df_agg_tran = get_aggregated_user()
            avg_value = agg_trans_avg(df_agg_tran)
            avg_value = pd.DataFrame(avg_value, columns=["avg_value"])
            df_av = pd.concat([df_agg_tran, avg_value], axis=1)
            v = df_av[(df_av["Year"] == year) & (df_av["Quarter"] == quarter)
                      & (df_av["State"] == state)]
            plt.figure(figsize=(12, 5))
            fig = px.scatter(v, x="Transaction_count", y="Transaction_amount",
                             size="avg_value", color="Transaction_type",
                             hover_name="Transaction_type", log_x=True, size_max=100)
            fig.update_layout(
                title_text="Bubble plot for transaction_count transaction_amount average_value for each type")
            st.write(fig)

        with col2:
            st.markdown("")
            new_v = new_frame(v)
            st.table(new_v)

        col1, col2 = st.columns(2)

        with col1:
            labels = v["Transaction_type"]
            values = v["Transaction_count"]
            fig = go.Figure(
                data=[go.Pie(labels=labels, values=values, hole=.5)])
            fig.update_layout(
                title_text="Transaction count with respect to transaction type")
            st.write(fig)

        with col2:
            labels = v["Transaction_type"]
            values = v["Transaction_amount"]
            fig = go.Figure(
                data=[go.Pie(labels=labels, values=values, hole=.5)])
            fig.update_layout(
                title_text="Transaction amount with respect to transaction type")
            st.write(fig)

        col1, col2 = st.columns(2)

        with col1:
            year_df = st.selectbox(label="Select year",
                                   options=(2018, 2019, 2020, 2021, 2022, 2023), index=0)

        with col2:
            transaction_type = st.selectbox(label="Select the transaction type",
                                            options=get_transaction_type(), index=0)

        col1, col2 = st.columns(2)

        with col1:
            df_agg_total = get_aggregated_user()
            df_agg_total = df_agg_total.groupby(["State", "Year", "Transaction_type"])[
                ["Transaction_count", "Transaction_amount"]].sum().reset_index()
            df_agg_avg = agg_trans_avg(df_agg_total)
            df_agg_avg = pd.DataFrame(df_agg_avg, columns=["Avg_value"])
            df_agg_total = pd.concat([df_agg_total, df_agg_avg], axis=1)
            q = df_agg_total[(df_agg_total["Year"] == year_df) & (
                df_agg_total["Transaction_type"] == transaction_type)]

            fig = px.bar(q, x='State', y='Transaction_count',
                         hover_data=['State', 'Transaction_count'], height=500, title="Transaction count state wise")
            st.write(fig)

        with col2:
            data = state_dict()
            state_li = state_list_val(data)
            fig = px.choropleth(q,
                                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                featureidkey='properties.ST_NM',
                                locations=state_li,
                                color='Transaction_count',
                                color_continuous_scale=px.colors.diverging.RdYlGn,
                                title="Transaction count state wise",
                                height=500)
            fig.update_geos(fitbounds='locations', visible=False)
            st.write(fig)

        col1, col2 = st.columns(2)

        with col1:
            df_agg_total = get_aggregated_user()
            df_agg_total = df_agg_total.groupby(["State", "Year", "Transaction_type"])[
                ["Transaction_count", "Transaction_amount"]].sum().reset_index()
            df_agg_avg = agg_trans_avg(df_agg_total)
            df_agg_avg = pd.DataFrame(df_agg_avg, columns=["Avg_value"])
            df_agg_total = pd.concat([df_agg_total, df_agg_avg], axis=1)
            q = df_agg_total[(df_agg_total["Year"] == year_df) & (
                df_agg_total["Transaction_type"] == transaction_type)]

            fig = px.bar(q, x='State', y='Transaction_amount',
                         hover_data=['State', 'Transaction_amount'], height=500, title="Transaction Amount state wise")
            st.write(fig)

        with col2:
            data = state_dict()
            state_li = state_list_val(data)
            fig = px.choropleth(q,
                                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                featureidkey='properties.ST_NM',
                                locations=state_li,
                                color='Transaction_amount',
                                color_continuous_scale=px.colors.diverging.RdYlGn,
                                title="Transaction Amount state wise",
                                height=500)
            fig.update_geos(fitbounds='locations', visible=False)
            st.write(fig)

        col1, col2 = st.columns(2)

        with col1:
            df_agg_total = get_aggregated_user()
            df_agg_total = df_agg_total.groupby(["State", "Year", "Transaction_type"])[
                ["Transaction_count", "Transaction_amount"]].sum().reset_index()
            df_agg_avg = agg_trans_avg(df_agg_total)
            df_agg_avg = pd.DataFrame(df_agg_avg, columns=["Avg_value"])
            df_agg_total = pd.concat([df_agg_total, df_agg_avg], axis=1)
            q = df_agg_total[(df_agg_total["Year"] == year_df) & (
                df_agg_total["Transaction_type"] == transaction_type)]

            fig = px.bar(q, x='State', y='Avg_value',
                         hover_data=['State', 'Avg_value'], height=500, title="Average value per transaction state wise")
            st.write(fig)

        with col2:
            data = state_dict()
            state_li = state_list_val(data)
            fig = px.choropleth(q,
                                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                featureidkey='properties.ST_NM',
                                locations=state_li,
                                color='Avg_value',
                                color_continuous_scale=px.colors.diverging.RdYlGn,
                                title="Average value per transaction state wise",
                                height=500)
            fig.update_geos(fitbounds='locations', visible=False)
            st.write(fig)

        transaction_type_total = st.selectbox(label="Select the transaction type wise",
                                              options=get_transaction_type(), index=0)

        col1, col2 = st.columns(2)

        with col1:

            df_agg_total = get_aggregated_user()
            df_agg_total = df_agg_total.groupby(["State", "Year", "Transaction_type"])[
                ["Transaction_count", "Transaction_amount"]].sum().reset_index()
            df_agg_avg = agg_trans_avg(df_agg_total)
            df_agg_avg = pd.DataFrame(df_agg_avg, columns=["Avg_value"])
            df_agg_total = pd.concat([df_agg_total, df_agg_avg], axis=1)
            year_df = df_agg_total.groupby(
                ["Year", "Transaction_type"]).sum().reset_index()
            year_df.drop(columns="State", inplace=True)
            y = year_df[year_df["Transaction_type"]
                        == transaction_type_total][:-1]

            fig = px.bar(y, x='Year', y='Transaction_count',
                         hover_data=['Year', 'Transaction_count'], color='Year', title="Year wise Total transaction count",
                         height=300)
            st.write(fig)

        with col2:

            df_agg_total = get_aggregated_user()
            df_agg_total = df_agg_total.groupby(["State", "Year", "Transaction_type"])[
                ["Transaction_count", "Transaction_amount"]].sum().reset_index()
            df_agg_avg = agg_trans_avg(df_agg_total)
            df_agg_avg = pd.DataFrame(df_agg_avg, columns=["Avg_value"])
            df_agg_total = pd.concat([df_agg_total, df_agg_avg], axis=1)
            year_df = df_agg_total.groupby(
                ["Year", "Transaction_type"]).sum().reset_index()
            year_df.drop(columns="State", inplace=True)
            y = year_df[year_df["Transaction_type"]
                        == transaction_type_total][:-1]

            fig = px.bar(y, x='Year', y='Transaction_amount',
                         hover_data=['Year', 'Transaction_amount'], color='Year', title="Year wise Total transaction Amount",
                         height=300)
            st.write(fig)

        col1, col2 = st.columns(2)

        with col1:

            fig = px.bar(y, x='Year', y='Avg_value',
                         hover_data=['Year', 'Avg_value'], color='Year', title="Year wise Average Amount per transaction",
                         height=300)
            st.write(fig)

        with col2:
            st.markdown("")
            st.markdown("")
            y = new_frame(y)
            st.table(y)
        st.markdown("#### Top 15 distircts")
        year_df_d = st.selectbox(label="Select year for the district wise data",
                                 options=(2018, 2019, 2020, 2021, 2022, 2023), index=0)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("#### Top 15 distircts for Transaction Count wise")
            df = get_map_transaction()
            df = df.groupby(["Year", "District"])[
                ["Transaction_count", "Transaction_amount"]].sum().reset_index()
            avg_value = agg_trans_avg(df)
            avg_value = pd.DataFrame(avg_value, columns=["avg_value"])
            df_av_dis = pd.concat([df, avg_value], axis=1)
            k = df_av_dis[df_av_dis["Year"] == year_df_d]
            c = k.sort_values(by=["Transaction_count"],
                              ascending=False).head(15)
            c = c[["Year", "District", "Transaction_count"]]
            c_df = new_frame(c)
            st.table(c_df)
        with col2:
            st.markdown("#### Top 15 distircts for Transaction Amount wise")
            df = get_map_transaction()
            df = df.groupby(["Year", "District"])[
                ["Transaction_count", "Transaction_amount"]].sum().reset_index()
            avg_value = agg_trans_avg(df)
            avg_value = pd.DataFrame(avg_value, columns=["avg_value"])
            df_av_dis = pd.concat([df, avg_value], axis=1)
            k = df_av_dis[df_av_dis["Year"] == year_df_d]
            c = k.sort_values(by=["Transaction_amount"],
                              ascending=False).head(15)
            c = c[["Year", "District", "Transaction_amount"]]
            c_df = new_frame(c)
            st.table(c_df)
        with col3:
            st.markdown("#### Top 15 distircts Per capita Transaction Amount")
            df = get_map_transaction()
            df = df.groupby(["Year", "District"])[
                ["Transaction_count", "Transaction_amount"]].sum().reset_index()
            avg_value = agg_trans_avg(df)
            avg_value = pd.DataFrame(avg_value, columns=["avg_value"])
            df_av_dis = pd.concat([df, avg_value], axis=1)
            k = df_av_dis[df_av_dis["Year"] == year_df_d]
            c = k.sort_values(by=["avg_value"],
                              ascending=False).head(15)
            c = c[["Year", "District", "avg_value"]]
            c_df = new_frame(c)
            st.table(c_df)

if selected == "Users-Insights":
    st.markdown("#### :black[USERS INSIGHTS]")
    col1, col2, col3 = st.columns(3)
    with col1:
        user_state = st.selectbox(label="Select the state users",
                                  options=state_list(), index=0)
    with col2:
        user_year = st.selectbox(label="Select the year users",
                                 options=year_list(), index=0)
    with col3:
        user_quarter = st.selectbox(
            label="Select the Quarter users", options=quarter_list(), index=0)

    col1, col2 = st.columns(2)

    with col1:
        user_df = get_agg_users()
        user = user_df[(user_df["State"] == user_state) & (
            user_df["Year"] == user_year) & (user_df["Quarter"] == user_quarter)]

        fig = px.scatter(user, x="User_percentage", y="User_count", title="bubble plot for users brands",
                         size="User_count", color="User_brand", log_x=True, size_max=60)

        st.write(fig)

    with col2:
        st.markdown("")
        user = new_frame(user)
        st.table(user)

    labels = user["User_brand"]
    values = user["User_count"]
    fig = go.Figure(
        data=[go.Pie(labels=labels, values=values, hole=.5)])
    fig.update_layout(
        title_text="Donut pie users Percentage brand wise")
    st.write(fig)

    col1, col2 = st.columns(2)

    with col1:

        dis_year = st.selectbox(label="Select the year",
                                options=(2019, 2020, 2021, 2022, 2023), index=0)

    with col2:

        dis_state = st.selectbox(label="Select the users state",
                                 options=state_list(), index=10)

    map_user = get_map_users()
    v = map_user.groupby(["State", "Year", "District"])[
        ["Registered_user", "App_opens"]].sum().reset_index()
    user_av = users_trans_avg(v)
    user_av = pd.DataFrame(user_av, columns=["App_open_per_person"])
    df_av_user = pd.concat([v, user_av], axis=1)
    u = df_av_user[(df_av_user["State"] == dis_state)
                   & (df_av_user["Year"] == dis_year)]

    fig = px.bar(u, x='District', y='App_opens', width=1500,
                 title="District wise App Opens")
    st.write(fig)

    fig = px.bar(u, x='District', y='Registered_user', width=1500,
                 title="District wise Registered Users")

    st.write(fig)

    fig = px.bar(u, x='District', y='App_open_per_person',
                 width=1500, title="District wise App Opens per person")
    st.write(fig)

    tot_state = st.selectbox(label="Select a state",
                             options=state_list(), index=10)

    col1, col2 = st.columns(2)

    with col1:

        tot_user = get_map_users()
        tot_user = tot_user.groupby(["State", "Year",])[
            ["Registered_user", "App_opens"]].sum().reset_index()
        av_tot = users_trans_avg(tot_user)
        av_tot = pd.DataFrame(av_tot, columns=["App_open_per_person"])
        df_tot_user = pd.concat([tot_user, av_tot], axis=1)
        to = df_tot_user[df_tot_user["State"] == tot_state][1:]
        fig = px.bar(to, x='Year', y='Registered_user', width=700, color="Year",
                     title="Year wise Registered Users")
        st.write(fig)

    with col2:

        fig = px.bar(to, x='Year', y='App_opens', width=700, color="Year",
                     title="Year wise App opens")
        st.write(fig)

    col1, col2 = st.columns(2)

    with col1:

        fig = px.bar(to, x='Year', y='App_open_per_person', width=700, color="Year",
                     title="Year wise App open per person")
        st.write(fig)

    with col2:
        st.markdown("")
        st.markdown("")
        to_df = new_frame(to)
        st.table(to_df)

    map_year = st.selectbox(label="Select an year",
                            options=(2019, 2020, 2021, 2022, 2023), index=0)

    tot_user = get_map_users()
    tot_user = tot_user.groupby(["State", "Year",])[
        ["Registered_user", "App_opens"]].sum().reset_index()
    av_tot = users_trans_avg(tot_user)
    av_tot = pd.DataFrame(av_tot, columns=["App_open_per_person"])
    df_tot_user = pd.concat([tot_user, av_tot], axis=1)
    mp = df_tot_user[df_tot_user["Year"] == map_year]

    data = state_dict()
    state_li = state_list_val(data)
    fig = px.choropleth(mp,
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations=state_li,
                        color='Registered_user',
                        color_continuous_scale="Viridis",
                        title="Registeres Users state wise",
                                height=1000, width=1200)
    fig.update_geos(fitbounds='locations', visible=False)
    st.write(fig)

    fig = px.choropleth(mp,
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations=state_li,
                        color='App_opens',
                        color_continuous_scale="Viridis",
                        title="App opens state wise",
                                height=1000, width=1200)
    fig.update_geos(fitbounds='locations', visible=False)
    st.write(fig)

    fig = px.choropleth(mp,
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations=state_li,
                        color='App_open_per_person',
                        color_continuous_scale="Viridis",
                        title="Per capita App open state wise",
                                height=1000, width=1200)
    fig.update_geos(fitbounds='locations', visible=False)
    st.write(fig)


##########################   xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx          ##########################################
