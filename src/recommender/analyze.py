import math
import multiprocessing
from itertools import combinations, chain
from collections import defaultdict
from common import batch, Progress, logger
from database import Repository


def analyze_products_by_user(data_set):
    repository = Repository(data_set=data_set)
    users = repository.get_users()
    count = 0
    total = len(users)
    for user_ids in batch(users, 100):
        users_products = []
        for user_id in user_ids:
            user_products = repository.get_products_bought_by_user(user_id)
            user_products = dict(
                user_id=user_id, 
                products=[dict(product_id=p['_id'], count=p['count']) 
                        for p in user_products])
            
            users_products.append(user_products)
            
            count += 1
            logger.info("{}/{}".format(count, total))

        repository.add_user_products(users_products)


def analyze_users_similarity(data_set, samples):
    repository = Repository(data_set=data_set)

    progress = Progress(math.ceil(((samples - 1) * samples) / 2))
    
    offset = 1
    for user_products1 in batch(repository.get_user_products(limit=samples-1), 100):
        for up1 in user_products1:
            max_similarity = 0.1
            similar = None
            for user_products2 in batch(repository.get_user_products(offset=offset, limit=samples-offset), 100):
                for up2 in user_products2:
                    progress.advance()
                    similarity, common, additional1, additional2 = calculate_products_similarity(up1['products'], up2['products'])
                    if similarity > max_similarity:
                        max_similarity = similarity
                        similar = dict(user1_id=up1['user_id'], user2_id=up2['user_id'], similarity=similarity, 
                                       common_products=common, add_products1=additional1, add_products2=additional2)
                        logger.info("{} {} {}".format(similar['user1_id'], similar['user2_id'], similarity))

            if similar is not None:
                repository.add_users_similarity(similar)

            offset += 1
            logger.info("{:.1f}% ETA {}".format(progress.get_progress(), progress.get_estimated_time()))


def analyze_users_similarity_multi(data_set, samples, user_id):
    repository = Repository(data_set=data_set)
    min_similarity = 0.2
    processes = 5
    pool = multiprocessing.Pool(processes=processes)
    step = math.ceil(samples / processes)
    user1 = repository.get_user_products(user_id=user_id)
    tasks = []
    for from_sample in range(0, samples, step):
        tasks.append((user1, data_set, min_similarity, from_sample, step))

    pool.map(_analyze_users_similarity, tasks)
    pool.close()
    pool.join()


def _analyze_users_similarity(args):
    user, data_set, min_similarity, offset, limit = args
    logger.info("{} {}".format(offset, limit))
    repository = Repository(data_set=data_set)
    progress = Progress(limit-1)
    for users in batch(repository.get_users_products(offset=offset, limit=limit-1), 1000):
        for user2 in users:
            if user['_id'] == user2['_id']:
                continue
            
            progress.advance()
            similarity, common, additional1, additional2 = calculate_products_similarity(user['products'], user2['products'])
            if similarity >= min_similarity:
                similar = dict(user1_id=user['user_id'], user2_id=user2['user_id'], similarity=similarity, 
                            common_products=common, add_products1=additional1, add_products2=additional2)
                
                repository.add_users_similarity(similar)

        logger.info("{:.1f}% ETA {}".format(progress.get_progress(), progress.get_estimated_time()))


def analyze_orders_similarity_multi(data_set, samples, orders, last_order_id, user_id):
    repository = Repository(data_set=data_set)
    progress = Progress(orders)
    min_similarity = 0.2
    # offset = 1

    processes = 5
    pool = multiprocessing.Pool(processes=processes)
    step = math.ceil(samples / processes)

    logger.info("Last order {}".format(last_order_id))

    for orders in batch(repository.get_orders_for_user(user_id=user_id), 10):
        tasks = []
        for order in orders:
            progress.advance()
            last_order_id = order['_id']
            for from_sample in range(0, samples, step):
                tasks.append((order, data_set, min_similarity, from_sample, step))

        logger.info("Last order {}".format(last_order_id))
        pool.map(_analyze_orders_similarity, tasks)
        logger.info("{:.1f}% ETA {}".format(progress.get_progress(), progress.get_estimated_time()))
        
    pool.close()
    pool.join()


def _analyze_orders_similarity(args):
    order, data_set, min_similarity, offset, limit = args
    logger.info("{} {} {}".format(order['_id'], offset, limit))
    repository = Repository(data_set=data_set)
    progress = Progress(limit-1)

    for orders2 in batch(repository.get_orders(offset=offset, limit=limit-1), 1000):
        for o2 in orders2:
            if o2['_id'] == order['_id']:
                continue

            progress.advance()
            similarity, common, additional1, additional2 = calculate_products_similarity(order['products'], o2['products'])
            if similarity >= min_similarity:
                similar = dict(order1_id=order['order_id'], user1_id=order['user_id'], order2_id=o2['order_id'], user2_id=o2['user_id'],
                                similarity=similarity, common_products=common, add_products1=additional1, add_products2=additional2)
                repository.add_orders_similarity(similar)

        logger.info("{:.1f}% ETA {}".format(progress.get_progress(), progress.get_estimated_time()))


def analyze_orders_similarity(data_set, samples):
    repository = Repository(data_set=data_set)
    progress = Progress(math.ceil(((samples - 1) * samples) / 2))
    similarity_threshold = 0.2
    offset = 1
    for orders1 in batch(repository.get_orders(limit=samples-1), 100):
        for o1 in orders1:
            max_similarity = similarity_threshold
            similar = None
            count = 0
            for orders2 in batch(repository.get_orders(offset=offset, limit=samples-offset), 100):
                for o2 in orders2:
                    progress.advance()
                    similarity, common, additional1, additional2 = calculate_products_similarity(o1['products'], o2['products'])
                    if similarity >max_similarity:
                        max_similarity = similarity
                        similar = dict(order1_id=o1['order_id'], user1_id=o1['user_id'], order2_id=o2['order_id'], user2_id=o2['user_id'],
                                       similarity=similarity, common_products=common, add_products1=additional1, add_products2=additional2)
                        logger.info("Similarity {} {} {}".format(similar['user1_id'], similar['user2_id'], similarity))

            if similar is not None:
                repository.add_orders_similarity(similar)

            offset += 1
            logger.info("{:.1f}% ETA {}".format(progress.get_progress(), progress.get_estimated_time()))


def calculate_products_similarity(products1, products2):
    set1 = set([p['product_id'] for p in products1])
    set2 = set([p['product_id'] for p in products2])
    common_products = set1.intersection(set2)

    products_count = defaultdict(int)
    for product in chain(products1, products2):
        products_count[product['product_id']] += int(product.get('count', 1))

    common_products_count = sum([products_count[p] for p in common_products])
    all_products_count = sum([products_count[p] for p in products_count.keys()])

    diff1 = set1.difference(set2)
    diff2 = set2.difference(set1)

    return common_products_count / all_products_count, list(common_products), list(diff1), list(diff2)


def analyze_products_totally(data_set):
    repository = Repository(data_set=data_set)
    global_products = repository.get_products_bought_globally()
    for product in global_products:
        repository.set_product_global(product['_id'], product['count'])
        logger.info("{} {}".format(product['_id'], product['count']))

# def analyze_orders_similarity_threaded(data_set, samples):
#     repository = Repository(data_set=data_set)
#     progress = Progress(math.ceil(((samples - 1) * samples) / 2))
#     min_similarity = 0.2
#     offset = 1
#     for orders1 in batch(repository.get_orders(limit=samples-1), 100):
#         threads = []
#         for o1 in orders1:
#             t = AnalyzeThread(data_set=data_set, samples=samples, min_similarity=min_similarity, offset=offset, order=o1)
#             threads.append(t)
#             t.start()
#             offset += 1

#         for t in threads:
#             t.join()

# from threading import Thread

# class AnalyzeThread(Thread):

#     def __init__(self, data_set, samples, min_similarity, offset, order):
#         super(AnalyzeThread, self).__init__()
#         self.data_set = data_set
#         self.samples = samples
#         self.offset = offset
#         self.order = order
#         self.max_similarity = min_similarity

#     def run(self):
#         logger.info("Thread {} {} {} {}".format(self.ident, self.order['order_id'], self.offset, self.samples-self.offset))
#         repository = Repository(data_set=self.data_set)
#         similar = None
#         for orders2 in batch(repository.get_orders(offset=self.offset, limit=self.samples-self.offset), 100):
#             for o2 in orders2:
#                 similarity, common, additional1, additional2 = calculate_products_similarity(self.order['products'], o2['products'])
#                 if similarity > self.max_similarity:
#                     self.max_similarity = similarity
#                     similar = dict(order1_id=self.order['order_id'], user1_id=self.order['user_id'], order2_id=o2['order_id'], user2_id=o2['user_id'],
#                                     similarity=similarity, common_products=common, add_products1=additional1, add_products2=additional2)
#                     logger.info("Similarity {} {} {}".format(similar['user1_id'], similar['user2_id'], similarity))

#         if similar is not None:
#             repository.add_orders_similarity(similar)

# def _analyze_orders_similarity_multi(data_set, samples, last_order_id):
#     repository = Repository(data_set=data_set)
#     progress = Progress(samples-1)
#     min_similarity = 0.2
#     offset = 1

#     # last_order_id = None
#     # last_order = repository.get_last_orders_similarity()
#     # if last_order:
#     #     print(last_order)
#     #     last_order_id = last_order['order1_id']
#     #     logger.info("First order {}".format(last_order_id))

#     pool = multiprocessing.Pool(processes=5)

#     print("Last order {}".format(last_order_id))

#     for orders1 in batch(repository.get_orders(limit=samples-1, last_order_id=last_order_id), 100):
#         tasks = []
#         for o1 in orders1:
#             last_order_id = o1['_id']
#             # logger.info("Order {}".format(last_order_id))
#             progress.advance()
#             tasks.append((o1, data_set, min_similarity, samples, offset, samples-offset))
#             offset += 1

#         print("Last order {}".format(last_order_id))
#         pool.map(analyze, tasks)
#         logger.info("{:.1f}% ETA {}".format(progress.get_progress(), progress.get_estimated_time()))

#     pool.close()
#     pool.join()

# def _analyze(args):
#     o1, data_set, min_similarity, samples, offset, limit = args
#     # logger.info("Start {} {}".format(offset, limit))

#     repository = Repository(data_set=data_set)
#     # similar = None
#     # max_similarity = min_similarity
#     # progress = Progress(samples - offset)
#     # count = 0

#     for orders2 in batch(repository.get_orders(limit=samples-offset, last_order_id=o1['_id']), 100):
#         for o2 in orders2:
#             # progress.advance()
#             similarity, common, additional1, additional2 = calculate_products_similarity(o1['products'], o2['products'])
#             if similarity >= min_similarity: # max_similarity:
#                 # max_similarity = similarity
#                 similar = dict(order1_id=o1['order_id'], user1_id=o1['user_id'], order2_id=o2['order_id'], user2_id=o2['user_id'],
#                                 similarity=similarity, common_products=common, add_products1=additional1, add_products2=additional2)
#                 # logger.info("Similarity {} {} {}".format(similar['user1_id'], similar['user2_id'], similarity))
#                 repository.add_orders_similarity(similar)

#         # logger.info("{:.1f}%".format(progress.get_progress()))

#     # if similar is not None:
#     #     repository.add_orders_similarity(similar)
#     # logger.info("Stop {} {}".format(offset, limit))
