#!/usr/bin/python

import config, json

def dump_data(filename, data):
    f = open(config.path + '/' + filename, 'w')
    f.write(json.dumps({'objects': data}))
    f.close()

def get_data(query, fetcher):
    cur = config.db.cursor()
    cur.execute(query)
    return [fetcher(row) for row in cur.fetchall()]

def gen_user(row):
    return {
        'id': row[0],
        'username': row[4],
        'fullname': row[1] + ' ' + row[2],
        'email': row[3]
    }

def gen_category(row):
    return {
        'id': row[0],
        'name': row[1],
        'parentId': row[2]
    }

query = "SELECT `id`, `first_name`, `last_name`, `email_address`, `username` FROM `sf_guard_user`"
dump_data('users.json', get_data(query, gen_user))

query = "SELECT `id`, `name`, `parent_id` FROM `category` WHERE `type` LIKE 'outcome'"
dump_data('outcome_categories.json', get_data(query, gen_category))

query = "SELECT `id`, `name`, `parent_id` FROM `category` WHERE `type` LIKE 'income'"
dump_data('income_categories.json', get_data(query, gen_category))
