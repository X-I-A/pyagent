import os
import gzip
import json
import base64
import logging
from typing import List, Dict, Union, Tuple
from xialib import Service
from xialib import BasicStorer
from xialib.adaptor import Adaptor
from xialib.storer import Storer


__all__ = ['Agent']


class Agent(Service):
    """Agent Application
    Receive data and save them to target database

    Attributes:
        sources (:obj:`list` of `Subscriber`): Data sources

    """
    log_level = logging.WARNING

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger("Agent")
        self.log_context = {'context': ''}
        self.logger.setLevel(self.log_level)

    def _parse_data(self, header: dict, data: Union[List[dict], str, bytes]) -> Tuple[str, dict, list]:
        if header['data_store'] != 'body':
            self.logger.error("Only data store type body is supported", extra=self.log_context)
            raise ValueError("AGT-000004")
        elif isinstance(data, list):
            tar_full_data = data
        elif header['data_encode'] == 'blob':
            tar_full_data = json.loads(data.decode())
        elif header['data_encode'] == 'b64g':
            tar_full_data = json.loads(gzip.decompress(base64.b64decode(data)).decode())
        elif header['data_encode'] == 'gzip':
            tar_full_data = json.loads(gzip.decompress(data).decode())
        else:
            tar_full_data = json.loads(data)

        if int(header.get('age', 0)) == 1:
            data_type = 'header'
        else:
            data_type = 'data'
        return data_type, header, tar_full_data