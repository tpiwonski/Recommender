from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId


class Repository(object):
    
    def __init__(self, db='dev', data_set='train'):
        self.client = MongoClient()
        self.db = self.client['recommender']
        self.data_set = data_set

    def add_products(self, products):
        collection = self.db['products']
        result = collection.insert_many(products)
        return result.inserted_ids
    
    def add_order_products(self, order_products):
        collection = self.db['order_products_' + self.data_set]
        result = collection.insert_many(order_products)
        return result.inserted_ids
    
    def add_orders(self, orders):
        collection = self.db['orders_' + self.data_set]
        result = collection.insert_many(orders)
        return result.inserted_ids
    
    def find_order_products(self, order_id):
        collection = self.db['order_products_' + self.data_set]
        result = collection.find(dict(order_id=order_id))
        results = list(result)
        result.close()
        return results
    
    def get_users(self):
        collection = self.db['orders_' + self.data_set]
        result = collection.distinct('user_id')
        return result

    def get_orders_for_user(self, user_id):
        collection = self.db['orders_' + self.data_set]
        cursor = collection.find(dict(user_id=user_id))
        return cursor

    def get_products_bought_by_user(self, user_id):
        collection = self.db['orders_' + self.data_set]
        # cursor = collection.find(dict(user_id=user_id), dict(order_id=1))
        cursor = collection.aggregate([
            {"$match": {"user_id": user_id}},
            {"$unwind": "$products"},
            {"$project": {"product_id": "$products.product_id"}},
            {"$group": {"_id": "$product_id", "count": {"$sum": 1}}},
            {"$sort": {"count": DESCENDING}}
        ])
        return cursor
    
    def get_products_bought_globally(self, limit=0):
        collection = self.db['orders_' + self.data_set]
        cursor = collection.aggregate([
            {"$unwind": "$products"},
            {"$project": {"product_id": "$products.product_id"}},
            {"$group": {"_id": "$product_id", "count": {"$sum": 1}}},
            {"$sort": {"count": DESCENDING}}
        ])
        return cursor

    def set_product_global(self, product_id, count):
        collection = self.db['products_globally_' + self.data_set]
        return collection.insert(dict(product_id=product_id, count=count))

    def get_product_bought_globally(self, product_id):
        collection = self.db['products_globally_' + self.data_set]
        result = collection.find_one(dict(product_id=product_id))
        return result
    
    def get_product(self, product_id):
        collection = self.db['products']
        result = collection.find_one(dict(product_id=product_id))
        return result

    def add_user_products(self, user_products):
        collection = self.db['user_products_' + self.data_set]
        result = collection.insert_many(user_products)
        return result.inserted_ids
    
    def get_users_products(self, offset=0, limit=0):
        collection = self.db['user_products_' + self.data_set]
        cursor = collection.find().sort("user_id", ASCENDING)
        if offset:
            cursor = cursor.skip(offset)
        
        if limit:
            cursor = cursor.limit(limit)

        return cursor
    
    def get_user_products(self, user_id):
        collection = self.db['user_products_' + self.data_set]
        result = collection.find_one(dict(user_id=user_id))
        return result
    
    def get_orders(self, offset=0, limit=0, last_order_id=None):
        collection = self.db['orders_' + self.data_set]
        if last_order_id:
            last_order_id = ObjectId(last_order_id)
            cursor = collection.find({"_id": {"$gt": last_order_id}})
        else:
            cursor = collection.find()

        cursor = cursor.sort("order_id", ASCENDING)
        if offset:
            cursor = cursor.skip(offset)
        
        if limit:
            cursor = cursor.limit(limit)

        return cursor

    def add_users_similarity(self, users_similarity):
        collection = self.db['users_similarity_' + self.data_set]
        return collection.insert(users_similarity)

    def add_orders_similarity(self, orders_similarity):
        collection = self.db['orders_similarity_' + self.data_set]
        return collection.insert(orders_similarity)

    def get_similar_orders_for_user(self, user_id):
        collection = self.db['orders_similarity_' + self.data_set]
        cursor = collection.find({"$or": [{"user1_id": user_id}, {"user2_id": user_id}]})
        return cursor

    def get_last_orders_similarity(self):
        collection = self.db['orders_similarity_' + self.data_set]
        cursor = collection.find().sort("order1_id", DESCENDING).limit(10)
        result = list(cursor)
        print(result)
        return result[0] if len(result) else None

    def get_similar_users_for_user(self, user_id):
        collection = self.db['users_similarity_' + self.data_set]
        cursor = collection.find({"$or": [{"user1_id": user_id}, {"user2_id": user_id}]})
        return cursor
