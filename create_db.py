import couchdb
from couchdb.design import ViewDefinition
from Scraper import scrape_data_tanks, scrape_data_alliances, postproces_alliances, postproces_tanks
 

couch = couchdb.Server("http://admin:admin@localhost:5984")

if 'tanks' in couch:
    #tanks = couch['tanks']
    del couch['tanks']
    tanks = couch.create('tanks')
else:
    tanks = couch.create('tanks')

if 'alliances' in couch:
    #alliances = couch['alliances']
    del couch['alliances']
    alliances = couch.create('alliances')
else:
    alliances = couch.create('alliances')

if 'combined' in couch:
    #alliances = couch['alliances']
    del couch['combined']
    combined = couch.create('combined')
else:
    combined = couch.create('combined')

df_tanks = scrape_data_tanks()
df_tanks = postproces_tanks(df_tanks)
for row in df_tanks.itertuples():
    
    tanks[str(row.Index +1)] = {'Id':str(row.Index +1),'Country':row.Country,'Type': row.Type, 'Quantity': row.Quantity, 'Origin': row.Origin}

df_alliances = scrape_data_alliances()
df_alliances = postproces_alliances(df_alliances)
for row in df_alliances.itertuples():
    if ( row.Countries == ['China', 'Egypt', 'France', 'Iran', 'Israel', 'Japan', 'Jordan', 'Pakistan', 'Saudi Arabia', 'Turkey', 'West Germany', 'United Kingdom', 'United States']):
        continue
    alliances[str(row.Index +1)] = {'Id':str(row.Index +1),'Name':row.Name,'Countries':row.Countries, 'Start':row.Start,'End':row.End}
    
#print (df_alliances)
#Mapping functions
def documentMapper(doc):
    if doc.get('Id'):
        _id = doc['Id']
        yield(_id, doc)

def threadCountMapper(doc):
    if doc.get('Country'):
        _author = doc['Country']
        yield(_author, 1)

def tank_orgin_mapper(doc):
    if doc.get('Id'):
        _country = doc['Country']
        _orgin = doc['Origin']
        _quantity = doc['Quantity']
        yield([_country,_orgin], int(_quantity))

def alliance_tanks_orgin_mapper(doc):
    if doc.get('Id'):
        _name = doc['Name']
        _orgin = doc['Origin']
        _quantity = doc['Quantity']
        yield([_name,_orgin], int(_quantity))

def alliance_tanks_type_mapper(doc):
    if doc.get('Id'):
        _name = doc['Name']
        _type = doc['Type']
        _quantity = doc['Quantity']
        yield([_name,_type], int(_quantity))

def overall_tank_quantity_mapper(doc):
    if doc.get('Id'):
        _type = doc['Type']
        _quantity = doc['Quantity']
        yield(_type, int(_quantity))

def overall_tank_orgin_mapper(doc):
    if doc.get('Id'):
        _orgin = doc['Origin']
        _quantity = doc['Quantity']
        yield(_orgin, int(_quantity))

def overall_alliance_tanks_quantity_mapper(doc):
    if doc.get('Id'):
        _name = doc['Name']
        _quantity = doc['Quantity']
        yield(_name, int(_quantity))
#Mapping reducing functions

def summingReducer(keys, values, rereduce):
    return sum(values)


#for row in tanks.view('_all_docs'):
#    print(row.id)

#View creation


view = ViewDefinition('index', 'thread_tank_view',documentMapper, language = 'python')
view.sync(tanks)

view = ViewDefinition('index', 'thread_alliance_view',documentMapper, language = 'python')
view.sync(alliances)

i=1
for a_vrow in alliances.view('index/thread_alliance_view'):
    a_row  = a_vrow.value
    a_countries = a_row.get('Countries')
    a_alliance = a_row.get('Name')
    a_start = a_row.get('Start')
    a_end = a_row.get('End')
    for country in a_countries:
        for t_vrow in tanks.view('index/thread_tank_view'): 
            t_row  = t_vrow.value
            t_country = t_row.get('Country')
            if t_country == country:
                t_type = t_row.get('Type')
                t_quantity = t_row.get('Quantity')
                t_orgin = t_row.get('Origin')
                combined[str(i)] = {'Id':str(i),'Country': t_country,'Type': t_type, 'Quantity': t_quantity, 'Origin': t_orgin,'Name':a_alliance, 'Start':a_start,'End':a_end}
                i+=1

view = ViewDefinition('index', 'thread_combined_view',documentMapper, language = 'python')
view.sync(combined)

#view for country and tank quantity and orgin
view = ViewDefinition('index', 'tank_orgin_view',tank_orgin_mapper, reduce_fun = summingReducer, language = 'python')
view.sync(tanks)

#view for overall - quantity of tanks 
view = ViewDefinition('index', 'overall_tank_type_quantity_view',overall_tank_quantity_mapper, reduce_fun = summingReducer, language = 'python')
view.sync(tanks)

#view for overall - tanks orgin 
view = ViewDefinition('index', 'overall_tank_orgin_quantity_view',overall_tank_orgin_mapper, reduce_fun = summingReducer, language = 'python')
view.sync(tanks)

#view for overall - quantity of tanks in alliances 
view = ViewDefinition('index', 'overall_alliance_tanks_quantity_view',overall_alliance_tanks_quantity_mapper, reduce_fun = summingReducer, language = 'python')
view.sync(combined)

#view for alliance - orgin and of quantity tanks 
view = ViewDefinition('index', 'alliance_tank_orgin_view',alliance_tanks_orgin_mapper, reduce_fun = summingReducer, language = 'python')
view.sync(combined)


view = ViewDefinition('index', 'alliance_tank_type_view',alliance_tanks_type_mapper, reduce_fun = summingReducer, language = 'python')
view.sync(combined)

