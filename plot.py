import matplotlib.pyplot as plt
import pandas as pd


def plot(vals):
    time, price, avg = [], [], []
    for x in vals:
        time.append(x[0])
        price.append(x[1])
        avg.append(x[2])

    prices = pd.DataFrame()
    prices['datetime'] = pd.to_datetime(time, unit='ms')
    prices['close'] = price
    prices['avg'] = avg
    prices = prices.set_index('datetime')
    print(prices.tail())
    prices.plot(grid=True)
    plt.show()
