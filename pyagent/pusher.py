import json
from typing import Dict, List, Union, Tuple
from xialib.adaptor import Adaptor
from xialib.storer import Storer
from xialib.subscriber import Subscriber
from pyagent.agent import Agent

__all__ = ['Pusher']

class Pusher(Agent):
    """Pusher Agent
    Push received data. Receive header = drop and create new table

    Attributes:
        sources (:obj:`list` of `Subscriber`): Data sources

    """
    def __init__(self,
                 storers: List[Storer],
                 adaptor_dict: Dict[str, Adaptor],
                 **kwargs):
        super().__init__(storers=storers, adaptor_dict=adaptor_dict, **kwargs)

    def _get_id_from_header(self, header: dict):
        source_id = header.get('source_id', header['table_id'])
        topic_id = header['topic_id']
        table_id = header['table_id']
        target_id = '.'.join(table_id.split('.')[:2])
        return source_id, topic_id, table_id, target_id

    def _get_adaptor_from_target(self, target_id: str):
        active_adaptor = self.adaptor_dict.get(target_id, None)
        if active_adaptor is None:
            self.logger.error("No adaptor for target id {}".format(target_id), extra=self.log_context)
            raise ValueError("AGT-000005")
        return active_adaptor

    def _push_header(self, header: dict, header_data: List[dict]) -> bool:
        source_id, topic_id, table_id, target_id = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        active_adaptor = self._get_adaptor_from_target(target_id)
        ctrl_info = active_adaptor.get_ctrl_info(source_id)
        old_fields = ctrl_info.get('FIELD_LIST', [])
        new_fields = [{key: value for key, value in line.items() if not key.startswith('_')} for line in header_data]

        # Case 1: New Table or New History
        if old_fields is None or ctrl_info.get('START_SEQ', None) != header['start_seq']:
            active_adaptor.drop_table(source_id)
            return active_adaptor.create_table(source_id, header['start_seq'],
                                               header.get('meta-data', dict()), header_data, False, table_id)
        # Case 2: Flexible Table Structure
        elif old_fields == active_adaptor.FLEXIBLE_FIELDS:  # pragma: no cover
            return True  # pragma: no cover
        # Case 3: Try to adapter fields
        else:
            for new_field in new_fields:
                new_field_name = new_field['field_name']
                old_field = [field for field in old_fields if field['field_name'] == new_field_name]
                old_field = old_field[0] if len(old_field) > 0 else None
                # Case 3.1: Impossible to adapter fields
                if old_field is None or old_field['key_flag'] != new_field['key_flag']:
                    active_adaptor.drop_table(source_id)
                    return active_adaptor.create_table(source_id, header['start_seq'],
                                                       header.get('meta-data', dict()), header_data, False, table_id)
                # Case 3.2: Field Type changes
                elif old_field['type_chain'] != new_field['type_chain']:
                    if not active_adaptor.alter_column(table_id, old_field, new_field):
                        active_adaptor.drop_table(source_id)
                        return active_adaptor.create_table(source_id, header['start_seq'],
                                                           header.get('meta-data', dict()), header_data,
                                                           False, table_id)
            return True

    def _raw_push_data(self, header: dict, body_data: List[dict]):
        source_id, topic_id, table_id, target_id = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        active_adaptor = self._get_adaptor_from_target(target_id)
        field_data = header['field_list']
        return active_adaptor.upsert_data(table_id, field_data, body_data)

    def _std_push_data(self, header: dict, body_data: List[dict]):
        source_id, topic_id, table_id, target_id = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        active_adaptor = self._get_adaptor_from_target(target_id)
        ctrl_info = active_adaptor.get_ctrl_info(source_id)
        field_data = ctrl_info.get('FIELD_LIST', None)
        return active_adaptor.upsert_data(table_id, field_data, body_data)

    def _age_push_data(self, header: dict, body_data: List[dict]):
        source_id, topic_id, table_id, target_id = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        active_adaptor = self._get_adaptor_from_target(target_id)
        ctrl_info = active_adaptor.get_ctrl_info(source_id)
        field_data = ctrl_info.get('FIELD_LIST', None)
        log_table_id = ctrl_info.get('LOG_TABLE_ID', '')
        if log_table_id == '':
            self.logger.warning("No Log Table, using std push", extra=self.log_context)  # pragma: no cover
            return self._std_push_data(header, body_data)  # pragma: no cover

        data_start_age = int(header['age'])
        data_end_age = int(header.get('end age', data_start_age))
        log_info = active_adaptor.get_log_info(source_id)
        loaded_log = [line for line in log_info if line['LOADED_FLAG'] == 'X']
        loaded_age = max([line['END_AGE'] for line in loaded_log]) if loaded_log else 1
        todo_list = [line for line in log_info if line['LOADED_FLAG'] != 'X']

        if data_end_age > loaded_age:
            age_list = list()
            for todo_item in todo_list:
                age_list = self._age_list_add_item(age_list, [todo_item['START_AGE'], todo_item['END_AGE']])

            todo_data = [line for line in body_data if not self._age_list_point_in(age_list, line['_AGE'])]
            if not active_adaptor.insert_raw_data(log_table_id, field_data, todo_data):
                self.logger.error("Log Table Insert Error", extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover
            log_data = {'SOURCE_ID': source_id, 'START_AGE': data_start_age, 'END_AGE': data_end_age, 'LOADED_FLAG': ''}
            if not active_adaptor.upsert_data(active_adaptor._ctrl_log_id, active_adaptor._ctrl_log_table, [log_data]):
                self.logger.error("Log Control Table Insert Error", extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover

            age_list = self._age_list_add_item(age_list, [data_start_age, data_end_age])
            age_list = self._age_list_set_start(age_list, loaded_age + 1)
            if (len(age_list)) > 0 and (age_list[0][0] <= loaded_age + 1 <= age_list[0][1]):
                if not active_adaptor.load_log_data(table_id, age_list[0][0], age_list[0][1]):
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
