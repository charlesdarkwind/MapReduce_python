from mrjob.job import MRJob
from mrjob.step import MRStep
from plot import plot


class FindMA(MRJob):
    '''Calculate a moving average series for a currency.

        usage:
        python find_ma.py --names=names.txt --period=20 --coin=ZRXBTC --names=names.txt ../prices2.csv
    '''

    def configure_options(self):
        super(FindMA, self).configure_options()
        self.add_passthrough_option(
            '--coin', type='str', default='BTCUSDT', help='Name of a specific coin to calc the MA')
        self.add_passthrough_option(
            '--period', type='int', default=20, help='period of the MA in hours')
        self.add_file_option('--names', help='Names of the coin')

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_get_prices,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_get_avgs),
            MRStep(mapper=self.mapper_regroup,
                   reducer=self.show_results)
        ]

    def mapper_init(self):
        with open('names.txt', 'U') as f:
            self.names = f.read().split('\n')

    # A line is the price for 69 coins and the time
    def mapper_get_prices(self, _, line):
        fields = line.split(',')
        time = fields[0]
        prices = fields[1:]
        btc_price = fields[1]

        if btc_price != 'BTCUSDT':  # Skip header
            for idx, price in enumerate(prices):
                coin = self.names[idx]

                if coin == self.options.coin:
                    if coin == 'BTUSDT':
                        yield coin, (int(time), float(btc_price))
                    else:  # Convert alt coin btc price to US Dollar
                        yield coin, (int(time), float(price) * float(btc_price))

    def reducer_init(self):
        # Calculate bin size of prices based on period
        period_in_ms = self.options.period * 1000 * 60 * 60
        # Div by 10k since prices were sampled every 10s
        self.options.bin = int(round(period_in_ms / 10000.0))

    def reducer_get_avgs(self, coin, values):
        prices = []
        for i, arr in enumerate(list(values)):
            time = arr[0]
            price = arr[1]
            prices.append(price)
            if i > self.options.bin:
                avg = sum(prices[i - self.options.bin:]) / self.options.bin
                yield coin, (time, price, avg)
            else:
                yield coin, (time, price, float('NaN'))

    # Those mapper & reducer are just to show the results
    def mapper_regroup(self, coin, values):
        yield coin, values  # Just regroup by keys

    def show_results(self, _, values):
        plot(list(values))


if __name__ == '__main__':
    FindMA.run()
