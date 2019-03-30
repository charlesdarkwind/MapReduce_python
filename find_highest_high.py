from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class HighestHigh(MRJob):
    '''Find the highest price of each coins and the time at wich it occured

        usage:
        python find_highest_high.py --names=names.txt ../prices2.csv
    '''

    def configure_options(self):
        super(HighestHigh, self).configure_options()
        self.add_file_option('--names', help='Names of the coin')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_prices,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_get_high)
        ]

    # A line is the price for 69 coins and the time
    def mapper_get_prices(self, _, line):
        fields = line.split(',')
        time = fields[0]
        prices = fields[1:]
        btc_price = fields[1]

        if btc_price != 'BTCUSDT':  # Skip header
            for idx, price in enumerate(prices):
                if idx != 0:
                    # Convert alt coin btc price to US Dollar
                    yield idx, (time, float(price) * float(btc_price))
                else:
                    yield idx, (time, float(btc_price))

    def reducer_init(self):
        with open('names.txt', 'U') as f:
            self.names = f.read().split('\n')

    def reducer_get_high(self, idx, values):
        _max = 0
        max_time = 0
        for time, price in values:
            if price > _max:
                _max = price
                max_time = time

        max_time_human = datetime.fromtimestamp(int(max_time)/1000.0)
        rounded_max = round(_max, 2)
        output = '{0} $    {1}'.format(rounded_max, max_time_human)

        yield self.names[idx], output


if __name__ == '__main__':
    HighestHigh.run()
