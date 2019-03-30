from mrjob.job import MRJob
from mrjob.step import MRStep


class BestReturns(MRJob):
    '''Find the currency that was the best investment for a given period

        usage:
        !python find_best_returns.py --names=names.txt --dates=dates.txt prices2.csv
    '''

    def configure_options(self):
        super(BestReturns, self).configure_options()
        self.add_file_option('--names', help='Names of the coin')
        self.add_file_option('--dates', help='Dates for the start of the period and for the end')

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_get_prices,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_get_returns),
            MRStep(reducer=self.reducer_get_best_return)
        ]

    def mapper_init(self):
        with open('dates.txt', 'U') as f:
            self.dates = f.read().split('\n')

    # A line is the price for 69 coins and the time
    def mapper_get_prices(self, _, line):
        fields = line.split(',')
        prices = fields[1:]
        time = fields[0]

        # Skip header
        if fields[1] != 'BTCUSDT':
            for idx, price in enumerate(prices):
                # Date of price must be in dates.txt
                if time == self.dates[0]:
                    yield idx, ('open', float(price))
                elif time == self.dates[1]:
                    yield idx, ('close', float(price))

    def reducer_init(self):
        with open('names.txt') as f:
            self.names = f.read().split('\n')

    def reducer_get_returns(self, idx, values):
        lst = list(values)
        profit = round(((lst[1][1] / lst[0][1]) - 1) * 100, 1)
        yield None, (self.names[idx], profit)

    def reducer_get_best_return(self, _, values):
        _max = 0
        coin_max = ''
        for coin, profit in values:
            if profit > _max:
                _max = profit
                coin_max = coin

        yield coin_max, '{0} %'.format(_max)


if __name__ == '__main__':
    BestReturns.run()
