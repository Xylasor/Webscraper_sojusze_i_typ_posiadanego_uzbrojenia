import couchdb
from couchdb.design import ViewDefinition
from prettytable import PrettyTable
from collections import defaultdict 


couch = couchdb.Server("http://admin:admin@localhost:5984")


tanks = couch['tanks']
alliances = couch['alliances']
combined = couch['combined']




#for row in tanks.view('index/tank_orgin_view', group_level = 2):
#    print(str(row.key) + " " + str(row.value))

def create_table(fields, data):
    pt = PrettyTable(field_names = fields)
    for f in fields:
        pt.align = 'l'
    data.sort(key=lambda x: int(x[1]),reverse=True)
    for row in data:
        pt.add_row([row[0], row[1]])
    return pt.get_string()

def create_table_1col(fields, data):
    pt = PrettyTable(field_names = fields)
    for f in fields:
        pt.align = 'l'
    data.sort()
    for row in data:
        pt.add_row([row])
    return pt.get_string()

#
#Queries
#

def country_tank_info(x_country, formated = False):
    '''Query for country - getting tanks information
    returns: tuple list of (tank_type, quantity)'''
    tank_quantity_list = []
    for vrow in tanks.view('index/thread_tank_view'):
        row  = vrow.value
        country = row.get('Country')
        if (country == x_country):
            tank_quantity_list.append((row.get('Type'), row.get('Quantity')))
    if not formated:
        return tank_quantity_list
    else:
        return create_table( ['Type' , 'Count'], tank_quantity_list)

def country_aliance_info(x_country, formated = False):
    '''Query for country - alliance  information
    returns: list of country alliances'''
    country_aliance_list = []
    for vrow in alliances.view('index/thread_alliance_view'):
        row  = vrow.value
        countries = row.get('Countries')
        if x_country in countries:
            country_aliance_list.append(row.get('Name'))
    if not formated:
        return country_aliance_list
    else:
        return create_table_1col(["Alliance"], country_aliance_list)

def country_tank_seller_origin_info(x_country, formated = False):
    '''Query for country - tanks orgin information
    returns: tuple list of (orgin, quantity)'''
    country_tank_seller_origin_list = []
    for vrow in tanks.view('index/tank_orgin_view', group_level = 2):
        row  = vrow.key
        country = row[0]
        if (country == x_country):
            country_tank_seller_origin_list.append((row[1], vrow.value))
    if not formated:
        return country_tank_seller_origin_list
    else:
        return create_table( ['Orgin' , 'Count'], country_tank_seller_origin_list)

def alliance_tanks_info(x_alliance, formated = False, max_end_date = 1900):
    '''Query for alliance - getting tanks information
    returns: tuple list of (tank_type, quantity)'''
    alliance_tank_list = []
    for vrow in alliances.view('index/thread_alliance_view'):
        row  = vrow.value
        alliance = row.get('Name')
        end = int(row.get('End'))
        if (alliance == x_alliance):
            if (end <= max_end_date):
                if not formated:
                    return []
                else:
                    return ["No available information"]
    for vrow in combined.view('index/alliance_tank_type_view', group_level = 2):
        row  = vrow.key
        alliance = row[0]
        if (alliance == x_alliance):
            alliance_tank_list.append((row[1], vrow.value))
        
    if not formated:
        return alliance_tank_list
    else:
        return create_table( ['Orgin' , 'Count'], alliance_tank_list)

def alliance_countries_info(x_alliance, formated = False):
    '''Query for alliance - getting member countries information
    returns: list of alliance members'''
    for vrow in alliances.view('index/thread_alliance_view'):
        row  = vrow.value
        alliance = row.get('Name')
        if alliance == x_alliance:
            if not formated:
                return row.get('Countries')
            else:
                return create_table_1col(["Country"], row.get('Countries'))

def alliance_tanks_origin_info(x_alliance, formated = False):
    '''Query for alliance - tanks orgin information
    returns: tuple list of (orgin, quantity)'''
    country_tank_seller_origin_list = []
    for vrow in combined.view('index/alliance_tank_orgin_view', group_level = 2):
        row  = vrow.key
        alliance = row[0]
        if (alliance == x_alliance):
            country_tank_seller_origin_list.append((row[1], vrow.value))
    if not formated:
        return country_tank_seller_origin_list
    else:
        return create_table( ['Orgin' , 'Count'], country_tank_seller_origin_list)


def overall_tanks_quantity(formated = False):
    '''Query for whole db - type quantity information
    returns: tuple list of (type, quantity)'''
    overall_tanks_quantity_list = []
    for vrow in tanks.view('index/overall_tank_type_quantity_view', group_level = 1):
        row  = vrow.key
        overall_tanks_quantity_list.append((row, vrow.value))
    if not formated:
        return overall_tanks_quantity_list
    else:
        return create_table( ['Orgin' , 'Count'], overall_tanks_quantity_list)

def overall_orgin_quantity(formated = False):
    '''Query for whole db - orgin quantity information
    returns: tuple list of (orgin, quantity)'''
    overall_orgin_quantity_list = []
    for vrow in tanks.view('index/overall_tank_orgin_quantity_view', group_level = 1):
        row  = vrow.key
        overall_orgin_quantity_list.append((row, vrow.value))
    if not formated:
        return overall_orgin_quantity_list
    else:
        return create_table( ['Orgin' , 'Count'], overall_orgin_quantity_list)

def overall_alliances_tank_quantity(formated = False):
    '''Query for whole db - orgin quantity information
    returns: tuple list of (orgin, quantity)'''
    overall_alliances_tank_quantity_list = []
    for vrow in combined.view('index/overall_alliance_tanks_quantity_view', group_level = 1):
        row  = vrow.key
        overall_alliances_tank_quantity_list.append((row, vrow.value))
    if not formated:
        return overall_alliances_tank_quantity_list
    else:
        return create_table( ['Orgin' , 'Count'], overall_alliances_tank_quantity_list)

def get_all_countries():
    all_countries = []
    for vrow in tanks.view('index/thread_tank_view'):
        row  = vrow.value
        country = row.get('Country')
        if country not in all_countries:
            all_countries.append(country)
    return sorted(all_countries)

def get_all_alliances():
    all_alliances = []
    for vrow in alliances.view('index/thread_alliance_view'):
        row  = vrow.value
        name = row.get('Name')
        if isinstance(name, list):
            name = name[0]
        if name not in all_alliances:
            all_alliances.append(name)
    return sorted(all_alliances)

def get_coutries_connections(start_date, end_date):
    all_coutries_connection = []
    for vrow in alliances.view('index/thread_alliance_view'):
        row  = vrow.value
        countries = row.get('Countries')
        end = row.get('End')
        start = row.get('Start')
        if (int(start) <= end_date and int(end) >= start_date ):
            all_coutries_connection += [[con1, con2] for con1 in countries for con2 in countries]
    return (all_coutries_connection)

def get_buyers_sellers_connections():
    all_buyers_seller_connection = []
    for vrow in tanks.view('index/thread_tank_view'):
        row  = vrow.value
        buyer = row.get('Country')
        seller = row.get('Origin')
        all_buyers_seller_connection.append([buyer, seller])
    return (all_buyers_seller_connection)
