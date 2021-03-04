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

    @classmethod
    def _age_list_add_item(cls, age_list: List[list], item: list) -> List[list]:
        new_age_list, cur_item, start_point = list(), item.copy(), None
        for list_item in age_list:
            # <List Item> --- <New Item>
            if list_item[1] + 1 < cur_item[0]:
                new_age_list.append(list_item)
            # <New Item> --- <List Item>
            elif cur_item[1] + 1 < list_item[0]:
                new_age_list.append(cur_item)
                cur_item = list_item.copy()
            # <New Item && List Item>
            else:
                cur_item = [min(cur_item[0], list_item[0]), max(cur_item[1], list_item[1])]
        new_age_list.append(cur_item)
        return new_age_list

    @classmethod
    def _age_list_set_start(cls, age_list: List[list], start_point: int) -> List[list]:
        new_age_list = list()
        for list_item in age_list:
            # <New Start Point> --- <Begin> --- <End>
            if start_point <= list_item[0]:
                new_age_list.append(list_item)
            # <Begin> --- <End> --- <New Start Point>
            elif list_item[1] < start_point:
                pass
            # <Begin> --- <New Start Point> --- <End>
            else:
                new_age_list.append([start_point, list_item[1]])
        return new_age_list

    @classmethod
    def _age_list_point_in(cls, age_list: List[list], point: int) -> bool:
        in_flag = any([list_item[0] <= point <= list_item[1] for list_item in age_list])
        return in_flag

    @classmethod
    def get_storer_register_dict(cls, storer_list: Dict[str, Storer]) -> Dict[str, Storer]:
        register_dict = dict()
        for storer in [v for k,v in storer_list.items()]:
            for store_type in storer.store_types:
                register_dict[store_type] = storer
        return register_dict

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger("Xeed")
        self.log_context = {'context': ''}
        self.logger.setLevel(self.log_level)

        default_storer = {"basic": BasicStorer()}
        if 'storer' in kwargs:
            if isinstance(kwargs['storer'], dict):
                default_storer.update(kwargs['storer'])
            else:
                default_storer.update({"custom": kwargs['storer']})
        self.storer_dict = self.get_storer_register_dict(default_storer)

    def _parse_data(self, header: dict, data: Union[List[dict], str, bytes]) -> Tuple[str, dict, list]:
        if header['data_store'] != 'body':
            active_storer = self.storer_dict.get(header['data_store'], None)
            if active_storer is None:
                self.logger.error("No storer for store type {}".format(header['data_store']), extra=self.log_context)
                raise ValueError("AGT-000004")
            header['data_store'] = 'body'
            tar_full_data = json.loads(gzip.decompress(active_storer.read(data)).decode())
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