#!/usr/bin/env python3

from pathlib import Path
import platform
import requests
import hashlib
import sqlite3
import json
import psutil
import time
import random
import urllib.parse
from werkzeug.utils import secure_filename


class InvalidExtensionError(BaseException):
    pass


class MeshChat:
    ALLOWED_FILETYPES = ['.txt', '.pdf', '.png', '.jpg', '.jpeg', '.gif', '.ipk', '.yml', '.yaml']

    def __init__(self, hostname="localnode.local.mesh", zone="MeshChat", sqlite_file=Path('/tmp/meshchat.db'), max_messages=500):
        self.hostname = hostname
        self.zone = zone
        self.db = sqlite3.connect(sqlite_file)
        self.db.row_factory = sqlite3.Row
        self.max_messages = max_messages
        self.version = "PyPiMeshChat 1.0"

        # TODO: Make this configurable
        self.filestore = Path('/Users/mcdermj/meshmap_files')
        self.timeout = 5.0

        self.base_uri = f'/meshchat?action='

        self.init_sqlite()

    def init_sqlite(self):
        cur = self.db.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS users (id TEXT, call_sign TEXT, epoch INTEGER, node TEXT, platform TEXT, UNIQUE(id, call_sign, node))')
        cur.execute('CREATE TABLE IF NOT EXISTS messages (id INTEGER UNIQUE, epoch INTEGER, message TEXT, call_sign TEXT, node TEXT, platform TEXT, channel TEXT)')
        cur.execute('CREATE TABLE IF NOT EXISTS nodes (name TEXT  UNIQUE, port INTEGER, messages_version INTEGER, alive INTEGER, last_polled INTEGER, non_mesh INTEGER)')
        cur.execute('CREATE TABLE IF NOT EXISTS params (key TEXT UNIQUE, value TEXT)')
        cur.execute('CREATE TABLE IF NOT EXISTS files (file TEXT, epoch INTEGER, size INTEGER, node TEXT, local INTEGER, platform TEXT, UNIQUE(file, node))')
        self.db.commit()

    @property
    def node(self):
        return platform.node()

    def get_node_list(self, alive=False, non_mesh=False):
        query_string = f"SELECT * FROM nodes {' WHERE alive = 1' if alive is True else ''}"
        cur = self.db.cursor()
        return [dict(row) for row in cur.execute(query_string)]

    def node_last_polled_since(self, node_name):
        cur = self.db.cursor()
        return cur.execute('SELECT last_polled FROM nodes WHERE name = ?', node_name).fetchone()[0]

    def refresh_node_list(self):
        services_url = f'http://{self.hostname}:8080/cgi-bin/sysinfo.json?services=1'
        services = requests.get(services_url, timeout=self.timeout).json()['services']

        node_list = [(link.hostname, link.port) for link in
                     [urllib.parse.urlparse(service['link']) for service in services if service['name'] == self.zone] if
                     link.path == '/meshchat']

        cur = self.db.cursor()
        cur.executemany('REPLACE INTO nodes (name, port, alive) VALUES (?, ?, 0)', node_list)
        for node in node_list:
            print(f'Polling node {node[0]}...')
            # TODO: Handle increased timeout for "non mesh" nodes
            # cur.execute("SELECT 1 FROM nodes WHERE last_polled < datetme('now', '-? seconds') AND name = ?", (300, node[0]))
            messages_version_url = f'http://{node[0]}.local.mesh:{node[1]}/cgi-bin/meshchat?action=messages_version'
            try:
                messages_version_result = requests.get(messages_version_url, timeout=self.timeout)
            except requests.exceptions.ConnectionError as e:
                print(f'Could not connect to {node[0]}: {e.args}')
                continue
            except requests.exceptions.ReadTimeout as e:
                print(f'Could not connect to {node[0]}: {e.args}')
                continue

            if messages_version_result.status_code != 200:
                cur.execute("UPDATE nodes SET last_polled = datetime('now'), non_mesh = 1, alive = 1 WHERE name = ?", (int(messages_version_result.text), node[0]))

            else:
                cur.execute('UPDATE nodes SET messages_version = ?, alive = 1 WHERE name = ?', (int(messages_version_result.text), node[0]))

        self.db.commit()

    def update_users(self, users):
        rows = [tuple(line.split('\t')) for line in users.splitlines()]
        cur = self.db.cursor()
        cur.executemany('REPLACE INTO users (call_sign, id, node, epoch, platform) VALUES (?, ?, ?, ?, ?)', rows)
        self.db.commit()

    def prune_messages(self):
        cur = self.db.cursor()

        message_count = cur.execute('SELECT count(1) FROM messages').fetchone()[0]
        messages_over_limit = message_count - self.max_messages
        if messages_over_limit > 0:
            print(f'Messages is over maximum limit by {messages_over_limit}, need to truncate')
            ids = [row['id'] for row in cur.execute('SELECT id FROM messages ORDER BY epoch DESC, id DESC LIMIT ?', (messages_over_limit,))]
            cur.execute( f"DELETE FROM messages WHERE id in ({','.join(['?'] * len(ids))})", ids)

        self.db.commit()

    def update_messages(self, messages):
        messages = [tuple([int(line.split('\t')[0], 16)] + line.split('\t')[1:7]) for line in messages.splitlines()]
        cur = self.db.cursor()
        try:
            cur.executemany('INSERT INTO messages (id, epoch, message, call_sign, node, platform, channel) VALUES(?, ?, ?, ?, ?, ?, ?)', messages)
        except sqlite3.IntegrityError:
            pass

        self.db.commit()

        self.prune_messages()

        # message_count = cur.execute('SELECT count(1) FROM messages').fetchone()[0]
        # print(f'{message_count} rows in database')
        # messages_over_limit = message_count - self.max_messages
        # if messages_over_limit > 0:
        #     print(f'Messages is over maximum limit by {messages_over_limit}, need to truncate')
        #     ids = [row['id'] for row in cur.execute('SELECT id FROM messages ORDER BY epoch DESC, id DESC LIMIT ?', (messages_over_limit,))]
        #     cur.execute( f"DELETE FROM messages WHERE id in ({','.join(['?'] * len(ids))})", ids)
        #
        # self.db.commit()

    def update_files(self, files):
        rows = [tuple(line.split('\t')) for line in files.splitlines()]
        cur = self.db.cursor()
        cur.executemany('REPLACE INTO files (file, node, size, epoch, platform, local) VALUES (?, ?, ?, ?, ?, 0)', rows)

        self.db.commit()

    def get_raw_users(self, local_only=True):
        cur = self.db.cursor()
        if local_only:
            return '\n'.join(['\t'.join(str(x) for x in row) for row in cur.execute("SELECT * FROM users WHERE node = ?", (self.node,))])
        else:
            return '\n'.join(['\t'.join(str(x) for x in row) for row in cur.execute("SELECT * FROM users")])

    def get_json_users(self):
        cur = self.db.cursor()
        return json.dumps([dict(row) for row in cur.execute('SELECT * FROM users ORDER BY epoch DESC')], indent=2)

    def get_raw_messages(self):
        cur = self.db.cursor()
        messages = [f'{row["id"]:08x}\t{row["epoch"]}\t{row["message"]}\t{row["call_sign"]}\t{row["node"]}\t{row["platform"]}\t{row["channel"]}' for row in cur.execute('SELECT * FROM messages ORDER BY epoch ASC, id ASC')]
        return '\n'.join(messages)

    def get_json_messages(self):
        cur = self.db.cursor()
        return json.dumps([{**dict(row), 'id': f'{row["id"]:08x}'} for row in cur.execute('SELECT * FROM messages ORDER BY epoch DESC, id DESC')], indent=2)

    def get_messages_dict(self):
        cur = self.db.cursor()
        return [{**dict(row), 'id': f'{row["id"]:08x}'} for row in cur.execute('SELECT * FROM messages ORDER BY epoch DESC, id DESC')]

    def get_raw_files(self, local_only=True):
        cur = self.db.cursor()
        query = f'SELECT file, node, size, epoch, platform FROM files{" WHERE local = 1" if local_only else ""}'
        return '\n'.join(['\t'.join(str(x) for x in row) for row in cur.execute(query)])

    def get_dict_files(self):
        cur = self.db.cursor()
        return [dict(row) for row in cur.execute('SELECT * FROM files ORDER BY epoch DESC')]

    def get_json_files(self):
        return json.dumps(self.get_dict_files(), indent=2)
        # cur = self.db.cursor()
        # return json.dumps([dict(row) for row in cur.execute('SELECT * FROM files ORDER BY epoch DESC')], indent=2)

    def refresh_users(self):
        for node in self.get_node_list(alive=True):
            self.fetch_raw_list(node, 'users_raw', self.update_users)

    def get_json_nodes(self):
        cur = self.db.cursor()
        return json.dumps([dict(row) for row in cur.execute("SELECT name AS node, IFNULL(CAST(strftime('%s', last_polled) as integer), 0) AS epoch FROM nodes ORDER BY epoch DESC")])

    def get_message_version(self):
        cur = self.db.cursor()
        version = cur.execute('SELECT sum(id) FROM messages').fetchone()[0]
        return 0 if version is None else version

    def get_file_stats(self):
        (total, used, free, _) = psutil.disk_usage(self.filestore)
        max_file_storage = int(total * 0.95)
        return {
            'total': total,
            'used': used,
            'files': used,
            'allowed': max_file_storage - used,
            'files_free': max_file_storage
        }

    def update_user_epoch(self, callsign, epoch):
        cur = self.db.cursor()
        cur.execute('UPDATE users SET epoch = ? WHERE call_sign = ? AND node = ?', (epoch, callsign, self.node))
        self.db.commit()

    def update_local_user(self, callsign, id, epoch):
        cur = self.db.cursor()
        cur.execute("REPLACE INTO users(call_sign, id, node, epoch, platform) VALUES (?, ?, ?, ?, 'pi')", (callsign, id, self.node, epoch))
        self.db.commit()

    def store_file(self, file):
        filename = self.filestore / Path(secure_filename(file.filename))
        if filename.suffix not in self.ALLOWED_FILETYPES:
            raise InvalidExtensionError(f'Filetype {filename.suffix} is not allowed')

        file.save(str(filename))

        cur = self.db.cursor()
        cur.execute("INSERT INTO files (file, epoch, size, node, local, platform) VALUES (?, ?, ?, ?, 1, 'pi')", (filename.name, int(time.time()), filename.stat().st_size, self.node))
        self.db.commit()

    def delete_file(self, file):
        filename = self.filestore / Path(file)

        print(filename.name)

        cur = self.db.cursor()
        cur.execute("DELETE FROM files WHERE file LIKE ?", (filename.name, ))
        self.db.commit()

        filename.unlink()

    def get_path_for_file(self, file):
        filename = self.filestore / Path(file)
        cur = self.db.cursor()
        rows = cur.execute("SELECT count(1) FROM files WHERE file LIKE ?", (filename.name, )).fetchall()
        if len(rows) < 1:
            raise FileNotFoundError(f'{filename.name} not found in filestore')

        return filename

    def create_message(self, callsign, message, channel, epoch):
        # TODO: Why won't a random 32-bit int work?
        idhash = int(hashlib.md5(f'{int(time.time())}.{random.randrange(0, 99999)}'.encode('utf-8')).hexdigest()[0:8], 16)

        cur = self.db.cursor()
        cur.execute("INSERT INTO messages (id, call_sign, epoch, message, node, platform, channel ) VALUES (?, ?, ?, ?, ?, 'pi', ?)", (idhash, callsign, epoch, message, self.node, channel))
        self.db.commit()

        self.prune_messages()

    @staticmethod
    def check_message_checksum(result):
        return False if 'Content-MD5' not in result.headers or \
                hashlib.md5(result.content).hexdigest() != result.headers['Content-MD5'] else True

    def refresh_messages(self):
        cur = self.db.cursor()
        for node in cur.execute('SELECT * FROM nodes WHERE messages_version != ? AND alive = 1', (self.get_message_version(),)):
            self.fetch_raw_list(node, 'messages_raw', self.update_messages)
        cur.execute("UPDATE nodes SET last_polled = datetime('now') WHERE alive = 1")
        self.db.commit()

    def refresh_files(self):
        cur = self.db.cursor()
        for node in self.get_node_list(alive=True):
            self.fetch_raw_list(node, 'local_files_raw', self.update_files)

    def refresh_all(self):
        print('Refreshing nodes...')
        self.refresh_node_list()

        print(f'Messages Version: {self.get_message_version()}')

        print('Refreshing messages...')
        self.refresh_messages()

        print('Refreshing users...')
        self.refresh_users()

        print('Refreshing files...')
        self.refresh_files()

    def fetch_raw_list(self, node, action, update_function):
        print(f'Processing {update_function.__name__} on {node["name"]}...')

        url = f'http://{node["name"]}.local.mesh:{node["port"]}/cgi-bin/meshchat?action={action}'

        try:
            result = requests.get(url, timeout=self.timeout)
        except requests.exceptions.ConnectionError as e:
            print(f'Could not connect to {node["name"]}: {e.args}')
            return

        if result.status_code == 404:
            print(f'Non mesh node: {node["name"]}')
            return

        if self.check_message_checksum(result) and len(result.content) > 0:
            update_function(result.text)


if __name__ == '__main__':
    chat = MeshChat()

    chat.refresh_node_list()

    # chat.refresh_all()
    #
    # print(chat.get_json_files())
