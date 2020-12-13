import os
import json
import pytest
import sqlite3
from xialib import SQLiteAdaptor, BasicStorer
from pyagent.pusher import Pusher

with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
    header_data = json.load(fp)
header_header = {'topic_id': 'test', 'table_id': '...aged_data', 'aged': 'True', 'data_store': 'body',
                 'data_encode': 'flat', 'age': '1', 'start_seq': '20201113222500000000'}

with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
    body_data = json.load(fp)

body_header = {'topic_id': 'test', 'table_id': '...aged_data', 'age': '2', 'start_seq': '20201113222500000000',
               'data_store': 'body', 'data_encode': 'flat'}

@pytest.fixture(scope='module')
def pusher():
    if os.path.exists(os.path.join('.', 'sqlite3', 'pusher.db')):
        os.remove(os.path.join('.', 'sqlite3', 'pusher.db'))
    conn = sqlite3.connect(os.path.join('.', 'sqlite3', 'pusher.db'))
    adaptor = SQLiteAdaptor(connection=conn)
    adaptor.create_table(SQLiteAdaptor._ctrl_table_id, '', dict(), SQLiteAdaptor._ctrl_table)
    storer = BasicStorer()
    adaptor_dict = {'.': adaptor}
    pusher = Pusher(storers=[storer], adaptor_dict=adaptor_dict)
    yield pusher

def test_simple_push(pusher):
    assert pusher.push_data(header_header, header_data)
    assert pusher.push_data(body_header, body_data)