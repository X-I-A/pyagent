import json
import logging
from typing import Dict, List, Union, Tuple
from pyagent.agent import Agent

__all__ = ['Pusher']

class Pusher(Agent):
    """Pusher Agent
    Push received data. Receive header = drop and create new table

    """
    def __init__(self, adaptor, **kwargs):
        super().__init__(adaptor=adaptor, **kwargs)
        self.logger = logging.getLogger("Agent.Pusher")
        if len(self.logger.handlers) == 0:
            log_format = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                           '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(log_format)
            self.logger.addHandler(console_handler)

        # Unique adaptor support
        if isinstance(self.adaptor, dict):
            self.adaptor = [v for k, v in self.adaptor.items()][0]

    def _get_id_from_header(self, header: dict):
        source_id = header.get('source_id', header['table_id'])
        topic_id = header['topic_id']
        table_id = header['table_id']
        segment_id = header.get('segment_id', "")
        return source_id, topic_id, table_id, segment_id

    def _push_header(self, header: dict, header_data: List[dict]) -> bool:
        source_id, topic_id, table_id, segment_id = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        return self.adaptor.create_table(table_id, header['start_seq'],
                                         header.get('meta-data', dict()), header_data, False, source_id)

    def _raw_push_data(self, header: dict, body_data: List[dict]):
        source_id, topic_id, table_id, segment_id = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        field_data = header['field_list']
        return self.adaptor.upsert_data(table_id, field_data, body_data)

    def _std_push_data(self, header: dict, body_data: List[dict]):
        source_id, topic_id, table_id, segment_id = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        ctrl_info = self.adaptor.get_ctrl_info(table_id)
        field_data = ctrl_info.get('FIELD_LIST', None)
        return self.adaptor.upsert_data(table_id, field_data, body_data)

    def _age_push_data(self, header: dict, body_data: List[dict]):
        source_id, topic_id, table_id, segment_id = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        ctrl_info = self.adaptor.get_ctrl_info(table_id)
        field_data = ctrl_info.get('FIELD_LIST', None)
        log_table_id = ctrl_info.get('LOG_TABLE_ID', '')
        if log_table_id == table_id:
            self.logger.info("Table logs itself, using std push", extra=self.log_context)  # pragma: no cover
            return self._std_push_data(header, body_data)  # pragma: no cover

        data_start_age = int(header['age'])
        data_end_age = int(header.get('end_age', data_start_age))
        log_info = self.adaptor.get_log_info(table_id, segment_id)
        loaded_log = [line for line in log_info if line['LOADED_FLAG'] == 'X']
        loaded_age = max([line['END_AGE'] for line in loaded_log]) if loaded_log else 1
        todo_list = [line for line in log_info if line['LOADED_FLAG'] != 'X']

        if data_end_age > loaded_age:
            age_list = list()
            for todo_item in todo_list:
                age_list = self._age_list_add_item(age_list, [todo_item['START_AGE'], todo_item['END_AGE']])

            todo_data = [line for line in body_data if not self._age_list_point_in(age_list, line['_AGE'])]
            if not self.adaptor.insert_raw_data(log_table_id, field_data, todo_data):
                self.logger.error("Log Table Insert Error", extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover
            log_data = {'TABLE_ID': table_id, "SEGMENT_ID": segment_id,
                        'START_AGE': data_start_age, 'END_AGE': data_end_age, 'LOADED_FLAG': ''}
            if not self.adaptor.upsert_data(self.adaptor._ctrl_log_id, self.adaptor._ctrl_log_table, [log_data]):
                self.logger.error("Log Control Table Insert Error", extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover

            age_list = self._age_list_add_item(age_list, [data_start_age, data_end_age])
            age_list = self._age_list_set_start(age_list, loaded_age + 1)
            if (len(age_list)) > 0 and (age_list[0][0] <= loaded_age + 1 <= age_list[0][1]):
                if not self.adaptor.load_log_data(table_id, age_list[0][0], age_list[0][1], segment_id):
                    self.logger.error("Load log table error", extra=self.log_context)  # pragma: no cover
                    return False  # pragma: no cover
            return True

    def push_data(self, header: dict, data: Union[List[dict], str, bytes], **kwargs) -> bool:
        data_type, data_header, data_body = self._parse_data(header, data)
        # Case 1: Header data
        if data_type == 'header':
            return self._push_header(data_header, data_body)
        # Case 2.1: Already Prepared data, raw insert
        elif header.get('raw_insert', False) and 'field_list' in header:
            return self._raw_push_data(data_header, data_body)
        # Case 2.2: Age insert data
        elif 'age' in header:
            return self._age_push_data(data_header, data_body)
        # Case 2.3: Standar insert
        else:
            return self._std_push_data(data_header, data_body)
