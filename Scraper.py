import requests
import urllib.request
import time
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import re

def scrape_data_tanks():
    url = 'https://en.wikipedia.org/wiki/List_of_main_battle_tanks_by_country?fbclid=IwAR0g7YWlmOIOpQ-Z6oPJUojZXk5jm_Z6-I67QB_qg5qIicCEQxL49bTpzGI'
    html = urlopen(url) 
    soup = BeautifulSoup(html, 'html.parser')


    tables = soup.find_all('table')

    countries = []
    tank_types = []
    quantities = []
    origins = []


    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 4 and cells[0]:
                if len(cells) == 4:
                    country = countries[-1]
                    countries.append(country)
                else:
                    country = cells[0]
                    countries.append(country.text.strip())

                tank_type, quantity, origin, _ = cells[-4:]


                tank_types.append(tank_type.text.strip())
                quantities.append(quantity.text.strip())
                origins.append(origin.text.strip())



    # Usuwanie odnosnikow i nawiasow
    countries = [re.sub("[\(\[].*?[\)\]]", "", elem) for elem in countries]
    tank_types = [re.sub("[\(\[].*?[\)\]]", "", elem) for elem in tank_types]
    quantities = [re.sub("[\(\[].*?[\)\]]", "", elem) for elem in quantities]
    origins = [re.sub("[\(\[].*?[\)\]]", "", elem) for elem in origins]

    # dictionary

    dict_tanks_2 = {'Country':countries, 'Type':tank_types, 'Quantity': quantities, 'Origin': origins}
    df_tanks = pd.DataFrame(dict_tanks_2)
    return df_tanks

# ---------------------------------------------------------------------------------------------
# Dane dotyczace sojuszy

def scrape_data_alliances():
    url_parent = 'https://en.wikipedia.org/wiki/List_of_military_alliances?fbclid=IwAR3k0PJ2F2mSv_FypCsdonLtqz4aW7ZNL2KKDXn8vj4BT93gpOdNOHDiQ38'

    html_parent = urlopen(url_parent) 
    soup_parent = BeautifulSoup(html_parent, 'html.parser')


    dots = soup_parent.find_all('li')


    alliances = []
    start_date = []
    end_date = []
    DATE = []
    delimiter = 'img alt'
    string_match = 'flagicon'
    historical_flag = 1 # Czy zaczyna sie strefa obecnych sojuszy
    date_prev = []

    for dot in dots:

        # Czy podpunkt zawiera kolejne podpunkty
        if not dot.find_all('li'):


            if historical_flag:
                titles_pre = str(dot).split('flagicon')
                titles = re.findall(r'title="(.*?)">', titles_pre[0])
            else:
                titles = re.findall(r'title="(.*?)">', str(dot))
                wiki_link = re.findall(r'href="(.*?)"', str(dot))
                # Specjalny przypadek, gdy pierwsza jest nazwa konkretnego kraju a nie sojusz
                if len(titles) > 0 and (titles[0] in ['United States', 'Australia', 'New Zealand', 'France', 'Estonia','United Kingdom']):
                    titles = []
                    wiki_link = []
                    dot_elems = [delimiter+e for e in str(dot).split(delimiter)]
                    alliance_name = []
                    alliance_count = []
                    for elem in dot_elems:
                        titl = re.findall(r'title="(.*?)">', elem)
                        if string_match not in elem:
                            alliance_name = titl
                            wiki_link = re.findall(r'href="(.*?)"', elem)
                        else:
                            alliance_count.append(titl)
                    titles = alliance_name + alliance_count

            if len(titles) >= 2:
                alliances.append(titles)
                date_new = re.findall(r'\d+',dot.text.strip())
                if historical_flag:
                    if len(date_new) >= 2:
                        start_date.append(date_new[0])
                        end_date.append(date_new[1])
                    elif len(date_new) == 1:
                        start_date.append(date_new[0])
                        end_date.append(date_new[0])
                    else:
                        if len(date) >= 2:
                            start_date.append(date[0])
                            end_date.append(date[1])
                        else:
                            start_date.append(date[0])
                            end_date.append(date[0])
                else:
                    if len(wiki_link) > 0 and ('wiki' in wiki_link[0]):
                        #print(wiki_link[0])
                        html_child = urlopen("https://en.wikipedia.org" + wiki_link[0])
                        soup_child = BeautifulSoup(html_child, 'html.parser')
                        wiki_text = soup_child.text.strip()
                        wiki_dates = re.findall(r'\d+',wiki_text)
                        if len(wiki_dates) > 0:
                            digit4_flag = 1
                            for wiki_date in wiki_dates:
                                if len(wiki_date) == 4:
                                    start_date.append(wiki_date)
                                    digit4_flag = 0
                                    break
                            if digit4_flag:
                                start_date.append('0')
                        else:
                            start_date.append('0')
                    else:
                        start_date.append('0')

                    end_date.append('3000')
                
                # Ustawienie flagi dla wspolczesnych sojuszy
                if titles[0] == 'Kosovo Force':
                    historical_flag = 0
                

        date = re.findall(r'\d+',dot.text.strip())
        if date:
            DATE.append(date)
            date_prev = re.findall(r'\d+',dot.text.strip())
        else:
            DATE.append(date_prev)
            date = date_prev
    

    alliance_name = [item[0] for item in alliances]
    # Lista panstw
    [item.pop(0) for item in alliances]

    #alliance_name = [re.sub("[\(\[].*?[\)\]]", "", elem) for elem in alliance_name]
    #alliances = [re.sub("[\(\[].*?[\)\]]", "", elem) for elem in alliances]


    dict_alliances = {'Name': alliance_name, 'Countries': alliances, 'Start': start_date, 'End': end_date}
    df_alliances = pd.DataFrame(dict_alliances)
    
    return df_alliances


#---------------------------------------------------



def postproces_tanks(df_tanks):
    df_tanks = df_tanks.replace('Korea, North', 'North Korea')
    df_tanks = df_tanks.replace('Korea, South', 'South Korea')
    df_tanks = df_tanks.replace('North Macedniona, Republic of', 'Macedonia')
    df_tanks = df_tanks.replace('TurkeyM60', 'Turkey')

    for index, row in df_tanks.iterrows():
        quantity = row.Quantity
        if quantity.find(',') != -1:
            quantity = quantity.replace(',','')

        quantity = re.findall(r'\d+', quantity)
        if len(quantity) == 0:
            df_tanks = df_tanks.drop([index])
        else:
            quantity = quantity[0]
            df_tanks.Quantity[index] = quantity
            
    for index, row in df_tanks.iterrows():
        origin = row.Origin
        origin = origin.replace('/','')

        if origin.find('United States') != -1:
            origin = 'UnitedStates'
        elif origin.find('Soviet Union') != -1:
            origin = 'SovietUnion'
        elif origin.find('Czech Republic') != -1:
            origin = 'CzechRepublic'
        elif origin.find('United Kingdom') != -1:
            origin = 'UnitedKingdom'
        elif origin.find('South Korea') != -1:
            origin = 'SouthKorea'
        elif origin.find('North Korea') != -1:
            origin = 'NorthKorea'

        origin = origin.split()

    
        if len(origin) == 0:
            df_tanks = df_tanks.drop([index])
        else:
            origin = origin[0]
            origin = re.sub(r'([A-Z])', r' \1', origin)[1:]
            df_tanks.Origin[index] = origin

    df_tanks = df_tanks.replace('Soviet Union', 'Russia')
    df_tanks.reset_index(drop = True, inplace = True)
    return df_tanks





def postproces_alliances(df_all):

    # Poprawki w nazwach sojuszy
    df_all['Name'] = [re.sub("[\(\[].*?[\)\]]", "", str(x)) for x in df_all['Name']]
    for index, row in df_all.iterrows():
        all_name = str(row.Name)
        len(all_name)
        if all_name.find('Samoa') != -1 or len(all_name) == 0:
            df_all = df_all.drop([index])
        else:
            df_all.Name[index] = all_name

    # Poprawki w liscie panstw
    for index, row in df_all.iterrows():
        count_list = row.Countries
        cnt_list = []
        for elem in count_list:
            if re.search('[A-Z]{2}', str(elem)):
                continue

            if elem == 'Soviet Union':
                elem = 'Russia'

            cnt_list.append(elem)


        if len(cnt_list) <= 1:
            df_all = df_all.drop([index])
        else:
            df_all.Countries[index] = cnt_list

    # Poprawki w datach
    for index, row in df_all.iterrows():
        if int(row.Start) > int(row.End):
            df_all = df_all.drop([index])

    # Usuwanie duplikatow
    df_all = df_all.drop_duplicates(subset=['Name'])

    # Sortowanie
    df_all = df_all.sort_values(by=['Name'])

    df_all.reset_index(drop = True, inplace = True)
    return df_all





#df_tanks = scrape_data_tanks()
#df_tanks1 = postproces_tanks(df_tanks)

#df_tanks.Quantity[0]
#type(df_tanks.Quantity[0])
#type(df_tanks1.Origin[0])
#df_tanks1.Quantity[0]

#df_all = scrape_data_alliances()
#df_all1 = postproces_alliances(df_all)

#type(df_all.End[0])
#ype(df_all1.End[0])