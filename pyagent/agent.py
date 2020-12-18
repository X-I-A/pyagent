import gzip
import json
import base64
import logging
from typing import List, Dict, Union, Tuple
from xialib.adaptor import Adaptor
from xialib.storer import Storer
from xialib.subscriber import Subscriber


__all__ = ['Agent']


class Agent():
    """Agent Application
    Receive data and save them to target database

    Attributes:
        sources (:obj:`list` of `Subscriber`): Data sources

    """
    log_level = logging.WARNING

    def __init__(self,
                 storers: List[Storer],
                 adaptor_dict: Dict[str, Adaptor],
                 **kwargs):
        self.logger = logging.getLogger("Agent")
        self.log_context = {'context': ''}
        self.logger.setLevel(self.log_level)

        if not all(isinstance(adaptor, Adaptor) for key, adaptor in adaptor_dict.items()):
            self.logger.error("adaptor should have type of Adaptor", extra=self.log_context)
            raise TypeError("AGT-000003")
        else:
            self.adaptor_dict = adaptor_dict

        if not all(isinstance(storer, Storer) for storer in storers):
            self.logger.error("storer should have type of Storer", extra=self.log_context)
            raise TypeError("AGT-000001")
        else:
            self.storers = storers
            self.storer_dict = self.get_storer_register_dict(storers)

        if 'sources' in kwargs:
            sources = kwargs
            if not all(isinstance(source, Subscriber) for source in sources):
                self.logger.error("source should have type of Subscriber", extra=self.log_context)
                raise TypeError("AGT-000002")
            else:  # pragma: no cover
                self.sources = sources  # pragma: no cover

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
    def get_storer_register_dict(cls, storer_list: List[Storer]) -> Dict[str, Storer]:
        register_dict = dict()
        for storer in storer_list:
            for store_type in storer.store_types:
                register_dict[store_type] = storer
        return register_dict
