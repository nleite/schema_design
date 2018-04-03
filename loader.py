#!/usr/bin/python
#-*- coding: utf-8 -*-
import pymongo
import os
import json
from bson.json_util import loads
fpath="./data/products_20k.json"

root_keys = ["name", "sku", "type", "_id", "categoryPath"]


def attributes_schema(d):
    """
    Takes a dictionary and applies the attributes pattern
    """
    doc = {"attributes": []}
    for k,v in d.iteritems():
        if k in root_keys:
            doc[k] = v
            continue
        doc["attributes"] .append({'k': k, 'v': v})

    return doc

def products_attributes(products):
    return map(attributes_schema, products)


def flat_original(line):
    """
    Generates a flat document from the original json file
    """
    return loads(line)



def match_category_product(category, product):
    ids = []
    for p in category['path']:
        ids.append(p["id"])
    for c in product['categoryPath']:
        if c["id"] in ids:
            return True
    return False



def subset_schema_light(category, products):
    """
    Same as `subset_schema` method but only keeping root_keys in the
    embeded top_products field.
    """
    top_products = []

    for p in products:
        if len(top_products) > 9:
            break
        if match_category_product(category, p):

            ligth_product = {}
            for k in root_keys:
                ligth_product[k] = p[k]

            ligth_product.pop("categoryPath")

            top_products.append(ligth_product)
    category["top_products"] = top_products

    return category



def subset_schema(category, products):
    """
    For each category, checks if any of the given products is whithin its categoryPath
    """
    top_products = []

    for p in products:
        if len(top_products) > 9:
            break
        if match_category_product(category, p):
            top_products.append(p)
    category["top_products"] = top_products

    return category


def get_filelines(filename):
    """
    Reads all json objects as strings
    """
    lines = []
    with open(filename) as fp:
        lines = fp.readlines()

    return lines


def process_products(filename):
    products = []
    for l in get_filelines(filename):
        products.append( flat_original(l) )

    return products


def process_categories(filename):
    categories = []
    for l in get_filelines(filename):
        categories.append( flat_original(l))

    return categories

def normalized(c, *args):
    return c

def store_categories(categories, products, schema, mc ):

    db = mc[schema.func_name]
    collection = db['categories']
    processed_categories = []
    for c in categories:
        processed_categories.append(schema(c, products))
    save_many( processed_categories, collection)
    save_many( products, db['products'])

def save_many(many, collection):
    """
    Writes to MongoDB the set of categories in the desired shape
    """
    collection.drop()
    collection.insert_many(many)

def store_products(products, categories, schema, mc):
    db = mc[schema.func_name]
    save_many( map( schema, products), db['products'] )
    save_many( categories, db['categories'] )


def polymorphic_schema(product):
    """
    Removes all null and empty array fields.
    """
    return {k: v for k,v in product.iteritems() if v not in [None, "", [] ]}


def main():

    pwd = os.environ["NHTT"]
    uri = "mongodb+srv://nhtt:{0}@nhtt-pytp8.mongodb.net/test".format(pwd)

    mc = pymongo.MongoClient(uri)
    print(mc.server_info())


    products = process_products("./data/products_10k.json")
    categories = process_categories("./data/categories.json")

    #store_products(products, categories, attributes_schema, mc)
    store_products(products, categories,  polymorphic_schema, mc)

    #store_categories(categories, products, normalized, mc)
    #store_categories(categories, products, subset_schema, mc)
    #store_categories(categories, products, subset_schema_light, mc)




main()

