import csv
from os import path

class Reader(object):

    def __init__(self, directory='instacart', data_set='train', orders='orders', products='products', order_products='order_products'):
        self.data_set = data_set
        self._orders = path.join(directory, "{}.csv".format(orders))
        self._products = path.join(directory, "{}.csv".format(products))
        self._order_products = path.join(directory, "{}__{}.csv".format(order_products, data_set))

    def load_products(self):
        with open(self._products) as products_file:
            products_reader = csv.DictReader(products_file)
            for product in products_reader:
                yield product
    
    def load_order_products(self):
        with open(self._order_products) as order_products_file:
            order_products_reader = csv.DictReader(order_products_file)
            for order_products in order_products_reader:
                yield order_products
    
    def load_orders(self):
        with open(self._orders) as orders_file:
            orders_reader = csv.DictReader(orders_file)
            for order in orders_reader:
                if order['eval_set'] == self.data_set:
                    yield order
