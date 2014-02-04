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
    def migrate_outcomes(self, users):
        cursor = self.sqldb.cursor()
        cursor.execute('SELECT id, category_id, amount, description, created_at, created_by FROM outcome')
        return { int(row[0]): self.mongodb.outcomes.insert({
            'category_id': int(row[1]),
            'amount': float(row[2]),
            'description': row[3],
            'created_at': row[4],
            'created_by': users[int(row[5])]
        }) for row in cursor }

    def migrate_incomes(self, users):
        cursor = self.sqldb.cursor()
        cursor.execute('SELECT id, category_id, amount, description, created_at, created_by FROM income')
        return { int(row[0]): self.mongodb.incomes.insert({
            'category_id': int(row[1]),
            'amount': float(row[2]),
            'description': row[3],
            'created_at': row[4],
            'created_by': users[int(row[5])]
        }) for row in cursor }

    def migrate_users(self):
        cursor = self.sqldb.cursor()
        cursor.execute('SELECT id, first_name, last_name, email_address, username, password, last_login, created_at FROM sf_guard_user')
        return { int(row[0]): self.mongodb.users.insert({
            'first_name': row[1],
            'last_name': row[2],
            'email': row[3],
            'username': row[4],
            'password': row[5],
            'last_login': row[6],
            'created_at': row[7]
        }) for row in cursor }

    def migrate_all(self):
        users = self.migrate_users()
        self.migrate_outcomes(users)
        self.migrate_incomes(users)

if __name__ == '__main__':
    m = Sql2Mongo(FINANCES_MYSQL)
    m.migrate_all()