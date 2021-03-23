import os
import json
import pytest
import random
import sqlite3
import time
from xialib import SQLiteAdaptor, SegmentFlower, BasicController
from pyagent.pusher import Pusher

sql_count_std = "SELECT COUNT(*) FROM std_data"
sql_count_aged = "SELECT COUNT(*) FROM aged_data"
sql_count_aged_s = "SELECT COUNT(*) FROM aged_s_data"
sql_count_aged_log = "SELECT COUNT(*) FROM XIA_aged_data"
sql_count_complex = "SELECT COUNT(*) FROM complex_data"
sql_count_complex_log = "SELECT COUNT(*) FROM XIA_complex_data"

@pytest.fixture(scope='module')
def pusher():
    if os.path.exists(os.path.join('.', 'sqlite3', 'pusher.db')):
        os.remove(os.path.join('.', 'sqlite3', 'pusher.db'))
    conn = sqlite3.connect(os.path.join('.', 'sqlite3', 'pusher.db'))
    adaptor = SQLiteAdaptor(db=conn)
    pusher = Pusher(adaptor=adaptor, controller=BasicController(min_frame_length=2))
    yield pusher

def test_std_push(pusher):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...std_data', 'aged': False, 'data_store': 'body',
                     'data_encode': 'flat', 'data_spec': 'x-i-a',  'data_format': 'record',
                     'age': '1', 'start_seq': '20201113222500000000'}
    body_header = {'topic_id': 'test', 'table_id': '...std_data', 'start_seq': '20201113222500000000',
                   'data_store': 'body', 'data_encode': 'flat'}
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        body_data = json.load(fp)

    assert pusher.push_data(header_header, header_data)
    assert pusher.push_data(body_header, body_data)

    c = pusher.adaptor.connection.cursor()
    c.execute(sql_count_std)
    assert c.fetchone() == (1000,)

def test_age_push(pusher):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...aged_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'data_spec': 'x-i-a', 'data_format': 'record',
                     'age': '1', 'start_seq': '20201113222500000000'}
    body_header = {'topic_id': 'test', 'table_id': '...aged_data', 'age': '2', 'start_seq': '20201113222500000000',
                   'data_store': 'body', 'data_encode': 'flat'}
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        body_data = json.load(fp)
        for body_line in body_data:
            body_line['_AGE'] = 2
    assert pusher.push_data(header_header, header_data)
    assert pusher.push_data(body_header, body_data)

    c = pusher.adaptor.connection.cursor()
    c.execute(sql_count_aged)
    assert c.fetchone() == (1000,)
    c.execute(sql_count_aged_log)
    assert c.fetchone() == (0,)

def test_complex_age_push(pusher):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...complex_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'age': '1', 'data_spec': 'x-i-a',  'data_format': 'record',
                     'start_seq': '20201113222500000000', "meta_data": {"frame_length": 2}}
    assert pusher.push_data(header_header, header_data)

    header_header = {'topic_id': 'test', 'table_id': '...complex_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'age': '1', 'data_spec': 'x-i-a',  'data_format': 'record',
                     'start_seq': '20201013222500000000', "meta_data": {"frame_length": 2}}
    assert pusher.push_data(header_header, header_data)

    header_header = {'topic_id': 'test', 'table_id': '...complex_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'age': '1', 'data_spec': 'x-i-a',  'data_format': 'record',
                     'start_seq': '20201213222500000000', "meta_data": {"frame_length": 2}}
    assert pusher.push_data(header_header, header_data)

    age_sample = random.sample(range(2, 1002), 1000)
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        body_data = json.load(fp)
    for age in age_sample:
        body_header = {'topic_id': 'test', 'table_id': '...complex_data', 'age': age, 'start_seq': '20201213222500000000',
                       'data_store': 'body', 'data_spec': 'x-i-a',  'data_format': 'record',
                       'data_encode': 'flat'}
        cur_data = body_data.pop()
        cur_data['_AGE'] = age
        assert pusher.push_data(body_header, [cur_data])
        if age % 10 == 0:
            tasks = pusher.get_tasks()
            for key, task in tasks.items():
                assert pusher.load_data(**task)
    time.sleep(2)
    tasks = pusher.get_tasks()
    for key, task in tasks.items():
        assert pusher.load_data(**task)

    c = pusher.adaptor.connection.cursor()
    c.execute(sql_count_complex)
    assert c.fetchone() == (1000,)
    c.execute(sql_count_complex_log)
    assert c.fetchone() == (0,)

def test_segment_age_push(pusher):
    segment_0 = {'id': '0', 'field_name': 'height', 'type_chain': ['int'], 'null': True}
    flower_0 = SegmentFlower(segment_0)
    segment_1 = {'id': '1', 'field_name': 'height', 'type_chain': ['int'], 'list': [150, 151, 152, 153]}
    flower_1 = SegmentFlower(segment_1)
    segment_2 = {'id': '2', 'field_name': 'height', 'type_chain': ['int'], 'min': 160, 'max': 169}
    flower_2 = SegmentFlower(segment_2)
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'data_spec': 'x-i-a', 'data_format': 'record',
                     'age': '1', 'start_seq': '20201113222500000000'}
    body_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'age': '2', 'start_seq': '20201113222500000000',
                   'data_store': 'body', 'data_encode': 'flat'}
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        body_data = json.load(fp)
        for body_line in body_data:
            body_line['_AGE'] = 2
    for header_line in header_data:
        if header_line['field_name'] == 'weight':
            header_line['type_chain'] = ['int', 'i_8']

    assert pusher.push_data(*flower_0.proceed(header_header, header_data))
    assert pusher.push_data(*flower_0.proceed(body_header, body_data))
    c = pusher.adaptor.connection.cursor()
    c.execute(sql_count_aged_s)
    assert c.fetchone() == (122,)

    assert pusher.push_data(*flower_1.proceed(header_header, header_data))
    assert pusher.push_data(*flower_1.proceed(body_header, body_data))
    c = pusher.adaptor.connection.cursor()
    c.execute(sql_count_aged_s)
    assert c.fetchone() == (186,)

    assert pusher.push_data(*flower_2.proceed(header_header, header_data))
    assert pusher.push_data(*flower_2.proceed(body_header, body_data))
    c = pusher.adaptor.connection.cursor()
    c.execute(sql_count_aged_s)
    assert c.fetchone() == (368,)

    header_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'data_spec': 'x-i-a', 'data_format': 'record',
                     'age': '1', 'start_seq': '20201213222500000000'}
    body_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'age': '2', 'start_seq': '20201213222500000000',
                   'data_store': 'body', 'data_encode': 'flat'}
    assert pusher.push_data(*flower_2.proceed(header_header, header_data))
    assert pusher.push_data(*flower_2.proceed(body_header, body_data))
    c = pusher.adaptor.connection.cursor()
    c.execute(sql_count_aged_s)
    assert c.fetchone() == (368,)

    # Obsolete Cases
    header_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'data_spec': 'x-i-a', 'data_format': 'record',
                     'age': '1', 'start_seq': '20201113222500000000'}
    body_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'age': '2', 'start_seq': '20201113222500000000',
                   'data_store': 'body', 'data_encode': 'flat'}
    assert pusher.push_data(*flower_2.proceed(header_header, header_data))
    assert pusher.push_data(*flower_2.proceed(body_header, body_data))
    c = pusher.adaptor.connection.cursor()
    c.execute(sql_count_aged_s)
    assert c.fetchone() == (368,)

    # Adapte Fields - Case 1: i_8 to int
    for header_line in header_data:
        if header_line['field_name'] == 'weight':
            header_line['type_chain'] = ['int']
    header_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'data_spec': 'x-i-a', 'data_format': 'record',
                     'age': '1', 'start_seq': '20201214222500000000'}
    pusher.adaptor.support_alter_column = False
    assert not pusher.push_data(*flower_2.proceed(header_header, header_data))
    pusher.adaptor.support_alter_column = True
    assert pusher.push_data(*flower_2.proceed(header_header, header_data))
    # Adapte Fields - Case 2: i_4 to i_8
    for header_line in header_data:
        if header_line['field_name'] == 'height':
            header_line['type_chain'] = ['int', 'i_8']
    header_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'data_spec': 'x-i-a', 'data_format': 'record',
                     'age': '1', 'start_seq': '20201215222500000000'}
    pusher.adaptor.support_alter_column = False
    assert not pusher.push_data(*flower_2.proceed(header_header, header_data))
    pusher.adaptor.support_alter_column = True
    assert pusher.push_data(*flower_2.proceed(header_header, header_data))
    # Adapte Fields - Case 3: Add a new field
    new_header_line = header_data[2].copy()
    new_header_line['field_name'] = 'extra'
    header_data.append(new_header_line)
    header_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'data_spec': 'x-i-a', 'data_format': 'record',
                     'age': '1', 'start_seq': '20201216222500000000'}
    pusher.adaptor.support_add_column = False
    assert not pusher.push_data(*flower_2.proceed(header_header, header_data))
    pusher.adaptor.support_add_column = True
    assert pusher.push_data(*flower_2.proceed(header_header, header_data))
    # Adapte Fields - Case 4: i_4 to char
    for header_line in header_data:
        if header_line['field_name'] == 'height':
            header_line['type_chain'] = ['char']
    header_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'data_spec': 'x-i-a', 'data_format': 'record',
                     'age': '1', 'start_seq': '20201217222500000000'}
    assert not pusher.push_data(*flower_2.proceed(header_header, header_data))

    # Segment 2 should have been purged
    c = pusher.adaptor.connection.cursor()
    c.execute(sql_count_aged_s)
    assert c.fetchone() == (186,)

def test_ko_cases(pusher: Pusher):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...aged_s_data', 'aged': True, 'data_store': 'body',
                     'data_encode': 'flat', 'data_spec': 'x-i-a', 'data_format': 'record',
                     'age': '1', 'start_seq': '20211113222500000000'}
    segment_2 = {'id': '2', 'field_name': 'height', 'type_chain': ['int'], 'min': 150, 'max': 169}
    flower_2 = SegmentFlower(segment_2)
    assert not pusher.push_data(*flower_2.proceed(header_header, header_data))

def test_object_init():
    conn = sqlite3.connect(os.path.join('.', 'sqlite3', 'pusher.db'))
    adaptor = SQLiteAdaptor(db=conn)
    pusher = Pusher(adaptor={"dummy": adaptor}, controller={"dummy": BasicController()})
    assert isinstance(pusher.adaptor, SQLiteAdaptor)
    assert isinstance(pusher.controller, BasicController)
