import networkx as nx
import numpy as np
import queries_db
import matplotlib.pyplot as plt
def draw_alliance_graph (start_date, end_date, k_core):
    '''państwa w sojuszach (państwa są węzłami; połączenie występuje gdy oba kraje są wspólnie w jakimkolwiek sojuszu) - 
    parametry filtrowanie po datach (wszyskie sojusze, które byly aktywne w danym okresie czasu) 
    filtrowanie po wielkosci k-rdzenia'''
    all_countries_connection = queries_db.get_coutries_connections(start_date, end_date) 
    G = nx.Graph()
    for elem in all_countries_connection:
        if isinstance(elem[0], list) and isinstance(elem[1], list) :
            G.add_edge(elem[0][0],elem[1][0])
        elif isinstance(elem[0], list):
            G.add_edge(elem[0][0],elem[1])
        elif isinstance(elem[1], list):
            G.add_edge(elem[0],elem[1][0])
        else:
            G.add_edge(elem[0],elem[1])

    G = nx.Graph(G)
    G.remove_edges_from(nx.selfloop_edges(G))
    G = nx.k_core(G, k_core)
    nx.draw(G, node_size=160, font_size=10, with_labels=True, node_color="forestgreen", font_color = "black",
            edge_color="skyblue")
    plt.show()


def draw_buyers_sellers_graph (k_core):
    '''sprzedaż kupno czołgów. Sieć państw lub sojuszy pokazująca czy między nimi dochodziło do sprzedaży uzbrojenia, 
    parametry filtrowanie po wielkosci k-rdzenia'''
    all_buyers_sellers_connection = queries_db.get_buyers_sellers_connections() #[buyer, seller]
    G = nx.Graph()
    for elem in all_buyers_sellers_connection:
        if isinstance(elem[0], list) and isinstance(elem[1], list) :
            G.add_edge(elem[0][0],elem[1][0])
        elif isinstance(elem[0], list):
            G.add_edge(elem[0][0],elem[1])
        elif isinstance(elem[1], list):
            G.add_edge(elem[0],elem[1][0])
        else:
            G.add_edge(elem[0],elem[1])
    G = nx.Graph(G)
    G.remove_edges_from(nx.selfloop_edges(G))
    G = nx.k_core(G, k_core)

    nx.draw(G, node_size=160, font_size=10, with_labels=True, node_color="forestgreen", font_color = "black",
            edge_color="skyblue")
    plt.show()
