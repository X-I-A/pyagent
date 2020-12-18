import os
import json
import pytest
import random
import sqlite3
from xialib import SQLiteAdaptor, BasicStorer
from pyagent.pusher import Pusher

@pytest.fixture(scope='module')
def pusher():
    if os.path.exists(os.path.join('.', 'sqlite3', 'pusher.db')):
        os.remove(os.path.join('.', 'sqlite3', 'pusher.db'))
    conn = sqlite3.connect(os.path.join('.', 'sqlite3', 'pusher.db'))
    adaptor = SQLiteAdaptor(connection=conn)
    adaptor.create_table(SQLiteAdaptor._ctrl_table_id, '', dict(), SQLiteAdaptor._ctrl_table)
    adaptor.create_table(SQLiteAdaptor._ctrl_log_id, '', dict(), SQLiteAdaptor._ctrl_log_table)
    storer = BasicStorer()
    adaptor_dict = {'.': adaptor}
    pusher = Pusher(storers=[storer], adaptor_dict=adaptor_dict)
    yield pusher

def test_age_push(pusher):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...aged_data', 'aged': 'True', 'data_store': 'body',
                     'data_encode': 'flat', 'age': '1', 'start_seq': '20201113222500000000'}
    body_header = {'topic_id': 'test', 'table_id': '...aged_data', 'age': '2', 'start_seq': '20201113222500000000',
                   'data_store': 'body', 'data_encode': 'flat'}
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        body_data = json.load(fp)
        for body_line in body_data:
            body_line['_AGE'] = 2
    assert pusher.push_data(header_header, header_data)
    assert pusher.push_data(body_header, body_data)

def test_std_push(pusher):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...std_data', 'aged': 'True', 'data_store': 'body',
                     'data_encode': 'flat', 'age': '1', 'start_seq': '20201113222500000000'}
    body_header = {'topic_id': 'test', 'table_id': '...std_data', 'start_seq': '20201113222500000000',
                   'data_store': 'body', 'data_encode': 'flat'}
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        body_data = json.load(fp)

    assert pusher.push_data(header_header, header_data)
    assert pusher.push_data(body_header, body_data)

def test_raw_push(pusher):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...raw_data', 'aged': 'True', 'data_store': 'body',
                     'data_encode': 'flat', 'age': '1', 'start_seq': '20201113222500000000'}

    body_header = {'topic_id': 'test', 'table_id': '...raw_data', 'age': '2', 'start_seq': '20201113222500000000',
                   'data_store': 'body', 'data_encode': 'flat', 'raw_insert': 'True',
                   'field_list': header_data}
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        body_data = json.load(fp)
    assert pusher.push_data(header_header, header_data)
    assert pusher.push_data(body_header, body_data)

def test_complex_age_push(pusher):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...complex_data', 'aged': 'True', 'data_store': 'body',
                     'data_encode': 'flat', 'age': '1', 'start_seq': '20201113222500000000'}
    assert pusher.push_data(header_header, header_data)

    age_sample = random.sample(range(2, 1002), 1000)
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        body_data = json.load(fp)
    for age in age_sample:
        body_header = {'topic_id': 'test', 'table_id': '...complex_data', 'age': age, 'start_seq': '20201113222500000000',
                       'data_store': 'body', 'data_encode': 'flat'}
        cur_data = body_data.pop()
        cur_data['_AGE'] = age
        assert pusher.push_data(body_header, [cur_data])

def test_change_header(pusher):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...header_data', 'aged': 'True', 'data_store': 'body',
                     'data_encode': 'flat', 'age': '1', 'start_seq': '20201113222500000000'}
    assert pusher.push_data(header_header, header_data)
    for header_line in header_data:
        if header_line['field_name'] == 'weight':
            header_line['type_chain'] = ['int', 'i_8']
    assert pusher.push_data(header_header, header_data)
    for header_line in header_data:
        if header_line['field_name'] == 'weight':
            header_line['type_chain'] = ['int', 'i_8']
    assert pusher.push_data(header_header, header_data)
    for header_line in header_data:
        if header_line['field_name'] == 'weight':
            header_line['type_chain'] = ['char']
    assert pusher.push_data(header_header, header_data)
    for header_line in header_data:
        if header_line['field_name'] == 'last_name':
            header_line['key_flag'] = False
    assert pusher.push_data(header_header, header_data)

def test_exceptions(pusher):
    with pytest.raises(ValueError):
        pusher._get_adaptor_from_target('dummy...table')