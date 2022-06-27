import matplotlib.pyplot as plt

def create_bar_plot(data, title):
    
    data = sorted(data, key=lambda x: int(x[1]), reverse=True)
    data = data[:10]
    data = [(tup[0],int(tup[1])) for tup in data] 
    fig = plt.figure(figsize=(8,8))
    plt.title(title)
    plt.bar(*zip(*data))
    plt.show()

