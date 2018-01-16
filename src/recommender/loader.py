from itertools import islice

from common import batch, logger
from inputdata import Reader
from database import Repository


def load_products(data_set):
    reader = Reader(data_set=data_set)
    repository = Repository(data_set=data_set)

    loaded = 0
    logger.info("Loading products")
    for products in batch(reader.load_products(), 100):
        repository.add_products(products)
        loaded += len(products)
        logger.info("Loaded products {}".format(loaded))

def load_order_products(data_set): 
    reader = Reader(data_set=data_set)
    repository = Repository(data_set=data_set)

    loaded = 0;
    logger.info("Loading order products")
    for order_products in batch(reader.load_order_products(), 100):
        repository.add_order_products(order_products)
        loaded += len(order_products)
        logger.info("Loaded order products {}".format(loaded))

def load_orders(data_set):
    reader = Reader(data_set=data_set)
    repository = Repository(data_set=data_set)

    loaded = 0
    logger.info("Loading orders")
    for orders in batch(reader.load_orders(), 100):
        orders_products = []
        for order in orders:                
            order_products = repository.find_order_products(order['order_id'])
            order_products = [p.copy() for p in order_products]
            if not order_products:
                continue

            order['products'] = order_products
            orders_products.append(order)
                            
        if orders_products:
            repository.add_orders(orders_products)

        loaded += len(orders_products)
        logger.info("Loaded orders {}".format(loaded))
