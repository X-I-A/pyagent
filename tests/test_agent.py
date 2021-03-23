import os
import json
import gzip
import base64
import pytest
from xialib import BasicStorer
from pyagent.agent import Agent


def test_parse_data():
    agt = Agent()
    # The same code has already been well tested in pyinsight Dispatcher Module
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
        header_data = json.load(fp)
    header_header = {'topic_id': 'test', 'table_id': '...aged_data', 'aged': True, 'data_store': 'body',
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
