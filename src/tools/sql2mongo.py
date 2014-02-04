from pymongo import MongoClient
import MySQLdb

FINANCES_MYSQL = {
    'user': 'duck',
    'passwd': '3CWSrOVLweG0',
    'db': 'duck_symfony',
    'host': 'localhost'
}

class Migrator(object):
    def __init__(self, sql_connection):
        self.sqldb = MySQLdb.connect(**sql_connection)
        cursor = self.sqldb.cursor()
        cursor.execute('SET NAMES utf8')
        self.mongodb = MongoClient().duck_finances

class Sql2Mongo(Migrator):
    def migrate_users(self):
        cursor = self.sqldb.cursor()
        cursor.execute('SELECT id, first_name, last_name, email_address, username, password, last_login, created_at FROM sf_guard_user')
        self.users = { int(row[0]): self.mongodb.users.insert({
            'first_name': row[1],
            'last_name': row[2],
            'email': row[3],
            'username': row[4],
            'password': row[5],
            'last_login': row[6],
            'created_at': row[7]
        }) for row in cursor }

    def migrate_outcomes(self):
        if not hasattr(self, 'users'):
            raise RuntimeError('users must be migrated before outcomes can be')
        cursor = self.sqldb.cursor()
        cursor.execute('SELECT id, category_id, amount, description, created_at, created_by FROM outcome')
        self.outcomes = { int(row[0]): self.mongodb.outcomes.insert({
            'category_id': int(row[1]),
            'amount': float(row[2]),
            'description': row[3],
            'created_at': row[4],
            'created_by': self.users[int(row[5])]
        }) for row in cursor }

    def migrate_incomes(self):
        if not hasattr(self, 'users'):
            raise RuntimeError('users must be migrated before outcomes can be')
        cursor = self.sqldb.cursor()
        cursor.execute('SELECT id, category_id, amount, description, created_at, created_by FROM income')
        self.incomes = { int(row[0]): self.mongodb.incomes.insert({
            'category_id': int(row[1]),
            'amount': float(row[2]),
            'description': row[3],
            'created_at': row[4],
            'created_by': self.users[int(row[5])]
        }) for row in cursor }

    def migrate_categories(self):
        cursor = self.sqldb.cursor()
        cursor.execute('SELECT id, parent_id, name, type, created_at, created_by FROM category')
        self.categories = {}
        for row in cursor:
            self.categories[int(row[0])] = self.mongodb.categories.insert({
                'parent_id': self.categories[row[1]] if row[1] else None,
                'name': row[2],
                'type': row[3],
                'created_at': row[4],
                'created_by': self.users[int(row[5])]
            })

    def migrate_all(self):
        self.migrate_users()
        self.migrate_categories()
        self.migrate_outcomes()
        self.migrate_incomes()

if __name__ == '__main__':
    m = Sql2Mongo(FINANCES_MYSQL)
    m.migrate_all()