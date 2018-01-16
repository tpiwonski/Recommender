from collections import defaultdict
from database import Repository
from common import batch, logger


def most_frequently_bought(data_set, user_id):
    repository = Repository(data_set=data_set)
    user_products = repository.get_products_bought_globally()
    recommended = sorted(user_products, key=lambda item: item['count'], reverse=True)[:20]
    for p in recommended:
        product = repository.get_product(p['_id'])
        if product:
            logger.info("{} {}".format(product['product_name'], p['count']))

def most_frequently_bought_by_user(data_set, user_id):
    repository = Repository(data_set=data_set)
    user_products = repository.get_products_bought_by_user(user_id)
    recommended = sorted(user_products, key=lambda item: item['count'], reverse=True)[:20]
    for p in recommended:
        product = repository.get_product(p['_id'])
        if product:
            logger.info("{} {}".format(product['product_name'], p['count']))

def most_frequently_bought_in_similar_orders(data_set, user_id):
    repository = Repository(data_set=data_set)
    orders = repository.get_similar_orders_for_user(user_id)
    recommend_products = defaultdict(float)
    common_products = defaultdict(float)
    count = 0
    for order in orders:
        if order['user1_id'] == user_id:
            products = order['add_products2']
        elif order['user2_id'] == user_id:
            products = order['add_products1']
        else:
            raise Exception()
        
        for p in products:
            recommend_products[p] += order['similarity']

        for p in order['common_products']:
            common_products[p] += 1

    logger.info("-- Most frequent common products:")
    common_products = sorted(common_products.items(), key=lambda item: item[1], reverse=True)[:10]
    for product_id, count in common_products:
        product = repository.get_product(product_id)
        if product:
            logger.info("{} {}".format(product['product_name'], count))

    logger.info("-- Recommended products:")
    recommended = sorted(recommend_products.items(), key=lambda item: item[1], reverse=True)[:10]
    for product_id, count in recommended:
        product = repository.get_product(product_id)
        if product:
            logger.info("{} {}".format(product['product_name'], count))


def most_frequently_bought_by_similar_users(data_set, user_id):
    repository = Repository(data_set=data_set)
    users = repository.get_similar_users_for_user(user_id=user_id)
    recommend_products = defaultdict(float)
    common_products = defaultdict(float)
    count = 0
    for user in users:
        if user['user1_id'] == user_id:
            products = user['add_products2']
        elif user['user2_id'] == user_id:
            products = user['add_products1']
        else:
            raise Exception()
        
        for p in products:
            recommend_products[p] += user['similarity']

        for p in user['common_products']:
            common_products[p] += 1

    logger.info("-- Most frequent common products:")
    common_products = sorted(common_products.items(), key=lambda item: item[1], reverse=True)[:10]
    for product_id, count in common_products:
        product = repository.get_product(product_id)
        if product:
            logger.info("{} {}".format(product['product_name'], count))

    logger.info("-- Recommended products:")
    recommended = sorted(recommend_products.items(), key=lambda item: item[1], reverse=True)[:10]
    for product_id, count in recommended:
        product = repository.get_product(product_id)
        if product:
            logger.info("{} {}".format(product['product_name'], count))
