import git
import os
import pandas as pd
import json
import requests
import mysql.connector as sql


# cloning github respository and making connection
def git_connect():
    try:
        git.Repo.clone_from(
            'https://github.com/PhonePe/pulse.git', 'phonepe_pulse_git')
    except:
        pass


git_connect()


# connecting to the local database

my_db = sql.connect(
    host='localhost',
    user='root',
    password='Venkat@123',
    database='phone_pe_pulse'
)

mycursor = my_db.cursor(buffered=True)


# data extraction from the gihub repo

class data_extract:
    def agg_transaction():
        try:
            path = 'phonepe_pulse_git/data/aggregated/transaction/country/india/state/'
            agg_state_list = os.listdir(path)

            data = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [],
                    'Transaction_count': [], 'Transaction_amount': []}

            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year_list = os.listdir(path_i)

                for j in agg_year_list:
                    path_j = path_i + j + "/"
                    agg_year_json = os.listdir(path_j)

                    for k in agg_year_json:
                        path_k = path_j + k
                        file = open(path_k, "r")
                        d = json.load(file)

                        for z in d['data']['transactionData']:
                            type = z['name']
                            count = z['paymentInstruments'][0]['count']
                            amount = z['paymentInstruments'][0]['amount']

                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['Transaction_type'].append(type)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)

            return data

        except:
            pass

    def aggregated_user():
        try:
            path = "phonepe_pulse_git/data/aggregated/user/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [],
                    'User_brand': [], 'User_count': [], 'User_percentage': []}
            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year_list = os.listdir(path_i)
                for j in agg_year_list:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)
                    for k in agg_year_json:
                        path_k = path_j + k
                        file = open(path_k, 'r')
                        d = json.load(file)
                        try:
                            for z in d['data']['usersByDevice']:
                                brand = z['brand']
                                count = z['count']
                                percentage = (z['percentage']*100)

                                data['State'].append(i)
                                data['Year'].append(j)
                                data['Quarter'].append(k[0])
                                data['User_brand'].append(brand)
                                data['User_count'].append(count)
                                data['User_percentage'].append(percentage)

                        except:
                            pass

            return data

        except:
            pass

    def map_transaction():
        try:
            path = "phonepe_pulse_git/data/map/transaction/hover/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [], 'District': [],
                    'Transaction_count': [], 'Transaction_amount': []}
            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year_list = os.listdir(path_i)
                for j in agg_year_list:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)
                    for k in agg_year_json:
                        path_k = path_j + k
                        f = open(path_k, 'r')
                        d = json.load(f)
                        for z in d['data']['hoverDataList']:
                            district = z['name'].split(' district')[0]
                            count = z['metric'][0]['count']
                            amount = z['metric'][0]['amount']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['District'].append(district)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)
            return data

        except:
            pass

    def map_user():
        try:
            path = "phonepe_pulse_git/data/map/user/hover/country/india/state/"
            agg_state_list = os.listdir(path)

            data = {'State': [], 'Year': [], 'Quarter': [],
                    'District': [], 'Registered_user': [], 'App_opens': []}

            for i in agg_state_list:
                path_i = path + i + "/"
                agg_year_list = os.listdir(path_i)

                for j in agg_year_list:
                    path_j = path_i + j + "/"
                    agg_year_json = os.listdir(path_j)
                    for k in agg_year_json:
                        path_k = path_j + k
                        file = open(path_k, "r")
                        d = json.load(file)

                        for z_key, z_value in d['data']['hoverData'].items():
                            district = z_key.split(' district')[0]
                            reg_user = z_value['registeredUsers']
                            app_opens = z_value['appOpens']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['District'].append(district)
                            data['Registered_user'].append(reg_user)
                            data['App_opens'].append(app_opens)
            return data

        except:
            pass

    def top_transaction_district():
        try:
            path = "phonepe_pulse_git/data/top/transaction/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [], 'District': [],
                    'Transaction_count': [], 'Transaction_amount': []}
            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year_list = os.listdir(path_i)
                for j in agg_year_list:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)
                    for k in agg_year_json:
                        path_k = path_j + k
                        f = open(path_k, 'r')
                        d = json.load(f)
                        for z in d['data']['districts']:
                            district = z['entityName']
                            count = z['metric']['count']
                            amount = z['metric']['amount']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['District'].append(district)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)
            return data
        except:
            pass

    def top_transaction_pincode():
        try:
            path = "phonepe_pulse_git/data/top/transaction/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],
                    'Transaction_count': [], 'Transaction_amount': []}
            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year_list = os.listdir(path_i)
                for j in agg_year_list:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)
                    for k in agg_year_json:
                        path_k = path_j + k
                        f = open(path_k, 'r')
                        d = json.load(f)
                        for z in d['data']['pincodes']:
                            pincode = z['entityName']
                            count = z['metric']['count']
                            amount = z['metric']['amount']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['Pincode'].append(pincode)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)
            return data
        except:
            pass

    def top_user_district():
        try:
            path = "phonepe_pulse_git/data/top/user/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [],
                    'District': [], 'Registered_user': []}
            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year_list = os.listdir(path_i)
                for j in agg_year_list:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)
                    for k in agg_year_json:
                        path_k = path_j + k
                        f = open(path_k, 'r')
                        d = json.load(f)
                        for z in d['data']['districts']:
                            district = z['name']
                            reg_user = z['registeredUsers']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['District'].append(district)
                            data['Registered_user'].append(reg_user)
            return data

        except:
            pass

    def top_user_pincode():
        try:
            path = "phonepe_pulse_git/data/top/user/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [],
                    'Pincode': [], 'Registered_user': []}
            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year_list = os.listdir(path_i)
                for j in agg_year_list:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)
                    for k in agg_year_json:
                        path_k = path_j + k
                        f = open(path_k, 'r')
                        d = json.load(f)
                        for z in d['data']['pincodes']:
                            pincode = z['name']
                            reg_user = z['registeredUsers']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['Pincode'].append(pincode)
                            data['Registered_user'].append(reg_user)
            return data

        except:
            pass


class data_transform:

    # transforming data related to transactions to pandas data frame
    aggregated_transaction = pd.DataFrame(data_extract.agg_transaction())
    map_transaction = pd.DataFrame(data_extract.map_transaction())
    top_transaction_district = pd.DataFrame(
        data_extract.top_transaction_district())
    top_transaction_pincode = pd.DataFrame(
        data_extract.top_transaction_pincode())

    # transforming data related to users to pandas data frame
    aggregated_users = pd.DataFrame(data_extract.aggregated_user())
    map_users = pd.DataFrame(data_extract.map_user())
    top_users_district = pd.DataFrame(data_extract.top_user_district())
    top_users_pincode = pd.DataFrame(data_extract.top_user_pincode())


def create_sql_table():

    # table creation for transactions details
  

    # table for aggregated_transaction
    mycursor.execute("""create table if not exists aggregated_transaction(
    State    varchar(255),
    Year     int,
    Quarter  int,
    Transaction_type  varchar(255),
    Transaction_count bigint,
    Transaction_amount bigint
     )""")

    # table for map_transaction
    mycursor.execute("""create table if not exists map_transaction(
    State    varchar(255),
    Year     int,
    Quarter  int,
    District varchar(100),
    Transaction_count bigint,
    Transaction_amount bigint
    )""")

    # table for top_transaction_district
    mycursor.execute("""create table if not exists top_transaction_district(
    State    varchar(255),
    Year     int,
    Quarter  int,
    District  varchar(255),
    Transaction_count bigint,
    Transaction_amount bigint
    )""")

    # table for top_transaction_pincode
    mycursor.execute("""create table if not exists top_transaction_pincode(
    State    varchar(255),
    Year     int,
    Quarter  int,
    Pincode  int,
    Transaction_count bigint,
    Transaction_amount bigint
    ) """)

    # table creation for users details

    # table for aggregated_users
    mycursor.execute("""create table if not exists aggregated_users(
    State    varchar(255),
    Year     int,
    Quarter  int,
    User_brand  varchar(255),
    User_count bigint,
    User_percentage float
    ) """)

    # table for map_users
    mycursor.execute("""create table if not exists map_users(
    State    varchar(255),
    Year     int,
    Quarter  int,
    District  varchar(255),
    Registered_user bigint,
    App_opens bigint
    ) """)

    # table for top_users_district
    mycursor.execute("""create table if not exists top_users_district(
    State    varchar(255),
    Year     int,
    Quarter  int,
    District  varchar(255),
    Registered_user bigint
    ) """)

    # table for top_users_pincode
    mycursor.execute("""create table if not exists top_users_pincode(
    State    varchar(255),
    Year     int,
    Quarter  int,
    Pincode  int,
    Registered_user bigint
    ) """)


create_sql_table()


class insert_values_into_tables():

    # inserting values into aggregated_transaction table

    def insert_to_aggregated_transaction():
        data = data_transform.aggregated_transaction.values.tolist()
        query = "insert into aggregated_transaction values(%s,%s,%s,%s,%s,%s)"
        for i in data:
            mycursor.execute(query, tuple(i))
        my_db.commit()

    # inserting values into map_transaction table
    def insert_to_map_transaction():
        data = data_transform.map_transaction.values.tolist()
        query = "insert into map_transaction values(%s,%s,%s,%s,%s,%s)"
        for i in data:
            mycursor.execute(query, tuple(i))
        my_db.commit()

    # inserting values into top_transaction_district table
    def insert_to_top_transaction_district():
        data = data_transform.top_transaction_district.values.tolist()
        query = "insert into top_transaction_district values(%s,%s,%s,%s,%s,%s)"
        for i in data:
            mycursor.execute(query, tuple(i))
        my_db.commit()

    # inserting values into top_transaction_district table
    def insert_to_top_transaction_pincode():
        data = data_transform.top_transaction_pincode.values.tolist()
        query = "insert into top_transaction_pincode values(%s,%s,%s,%s,%s,%s)"
        for i in data:
            mycursor.execute(query, tuple(i))
        my_db.commit()

    # inserting values into users table

     # inserting values into aggregated users table

    def insert_to_aggregated_users():
        data = data_transform.aggregated_users.values.tolist()
        query = "insert into aggregated_users values(%s,%s,%s,%s,%s,%s)"
        for i in data:
            mycursor.execute(query, tuple(i))
        my_db.commit()

    # inserting values into  map_users table
    def insert_to_map_users():
        data = data_transform.map_users.values.tolist()
        query = "insert into map_users values(%s,%s,%s,%s,%s,%s)"
        for i in data:
            mycursor.execute(query, tuple(i))
        my_db.commit()

    # inserting values into  top_users_district table
    def insert_to_top_users_district():
        data = data_transform.top_users_district.values.tolist()
        query = "insert into top_users_district values(%s,%s,%s,%s,%s)"
        for i in data:
            mycursor.execute(query, tuple(i))
        my_db.commit()

    # inserting values into  top_users_district table
    def insert_to_top_users_pincode():
        data = data_transform.top_users_pincode.values.tolist()
        query = "insert into top_users_pincode values(%s,%s,%s,%s,%s)"
        for i in data:
            mycursor.execute(query, tuple(i))
        my_db.commit()


def data_insertion_mysql():
    try:
        insert_values_into_tables.insert_to_aggregated_transaction()
        insert_values_into_tables.insert_to_map_transaction()
        insert_values_into_tables.insert_to_top_transaction_district()
        insert_values_into_tables.insert_to_top_transaction_pincode()
        insert_values_into_tables.insert_to_aggregated_users()
        insert_values_into_tables.insert_to_map_users()
        insert_values_into_tables.insert_to_top_users_district()
        insert_values_into_tables.insert_to_top_users_pincode()
    except:
        pass


data_insertion_mysql()
