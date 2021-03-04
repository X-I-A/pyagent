import os
import json
import gzip
import base64
import pytest
from xialib import BasicStorer
from pyagent.agent import Agent


def test_age_list():
    age_list = []
    assert not Agent._age_list_point_in(age_list, 1)
    age_list = Agent._age_list_add_item(age_list, [2, 2])
    age_list = Agent._age_list_add_item(age_list, [1, 1])
    assert age_list == [[1, 2]]
    age_list = Agent._age_list_add_item(age_list, [9, 10])
    assert age_list == [[1, 2], [9, 10]]
    age_list = Agent._age_list_add_item(age_list, [5, 5])
    age_list = Agent._age_list_add_item(age_list, [4, 4])
    assert age_list == [[1, 2], [4, 5], [9, 10]]
    assert Agent._age_list_set_start(age_list, 1) == [[1, 2], [4, 5], [9, 10]]
    age_list = Agent._age_list_set_start(age_list, 5)
    assert age_list == [[5, 5], [9, 10]]
    age_list = Agent._age_list_set_start(age_list, 5)
    assert age_list == [[5, 5], [9, 10]]
    assert Agent._age_list_point_in(age_list, 5)
    assert Agent._age_list_point_in(age_list, 10)
    assert Agent._age_list_point_in(age_list, 9)
    assert not Agent._age_list_point_in(age_list, 8)
    age_list = Agent._age_list_add_item(age_list, [1, 10])
    assert age_list == [[1, 10]]

def test_parse_data():
    agt = Agent()
    # The same code has already been well tested in pyinsight Dispatcher Module
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...aged_data', 'aged': 'True', 'data_store': 'body',
                         'data_encode': 'flat', 'age': '1', 'start_seq': '20201113222500000000'}
    assert isinstance(agt._parse_data(header_header, header_data)[2], list)
    header_header['data_encode'] = 'flat'
    assert isinstance(agt._parse_data(header_header, json.dumps(header_data))[2], list)
    header_header['data_encode'] = 'blob'
    assert isinstance(agt._parse_data(header_header, json.dumps(header_data).encode())[2], list)
    header_header['data_encode'] = 'b64g'
    assert isinstance(agt._parse_data(header_header,
                                      base64.b64encode(gzip.compress(json.dumps(header_data).encode())).decode())[2],
                      list)
    header_header['data_encode'] = 'gzip'
    assert isinstance(agt._parse_data(header_header, gzip.compress(json.dumps(header_data).encode()))[2], list)
    with open(os.path.join('.', 'input', 'person_simple', 'schema.gz'), 'wb') as fp:
        fp.write(gzip.compress(json.dumps(header_data).encode()))
    header_header['data_store'] = 'file'
    assert isinstance(agt._parse_data(header_header, os.path.join('.', 'input', 'person_simple', 'schema.gz'))[2], list)

    with pytest.raises(ValueError):
        header_header['data_store'] = 'gcs'
        agt._parse_data(header_header, header_data)

def test_exceptions():
    with pytest.raises(TypeError):
        agt = Agent(storer=object())
    with pytest.raises(TypeError):
        agt = Agent(adaptor={'err': object()})
    agt = Agent(storer=BasicStorer())
    agt = Agent(storer={"dummy": BasicStorer()})
