import json
import logging
from datetime import datetime
from typing import List, Union
from xialib import SegmentFlower
from pyagent.agent import Agent

__all__ = ['Pusher']


class Pusher(Agent):
    """Pusher Agent
    Push received data. Receive header = drop and create new table

    """
    def __init__(self, adaptor, controller, **kwargs):
        super().__init__(adaptor=adaptor, controller=controller, **kwargs)
        self.logger = logging.getLogger("Agent.Pusher")
        if len(self.logger.handlers) == 0:
            log_format = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                           '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(log_format)
            self.logger.addHandler(console_handler)

        # Unique adaptor / controller support
        if isinstance(self.adaptor, dict):
            self.adaptor = [v for k, v in self.adaptor.items()][0]
        if isinstance(self.controller, dict):
            self.controller = [v for k, v in self.controller.items()][0]

    def _get_largest_field_dict(self, ctrl_info_dict: dict) -> dict:
        return_dict = dict()
        all_existed_fields = [field for id, ctr in ctrl_info_dict.items() for field in ctr["field_list"]]
        field_names = [line['field_name'] for line in all_existed_fields]
        for field_name in field_names:
            type_chain = list()
            for field in all_existed_fields:
                if field['field_name'] == field_name:
                    if not type_chain:
                        type_chain = field['type_chain']
                    elif len(type_chain) > len(field['type_chain']):
                        type_chain = field['type_chain']
                    elif type_chain[-1] < field['type_chain'][-1]:
                        type_chain = field['type_chain']
            return_dict[field_name] = type_chain
        return return_dict

    def _get_id_from_header(self, header: dict):
        source_id = header.get('source_id', header['table_id'])
        topic_id = header['topic_id']
        table_id = header['table_id']
        if int(header.get("age", 0)) == 1:
            segment_id = header.get("meta_data", {}).get("segment", {}).get("id", "")
        else:
            segment_id = header.get('segment_id', "")
        ctrl_key = "/".join([table_id, segment_id, header['start_seq']])
        return source_id, topic_id, table_id, segment_id, ctrl_key

    def _create_table(self, header: dict, meta_data: dict, field_data: List[dict], drop: bool) -> bool:
        """Create Table
        """
        source_id, topic_id, table_id, segment_id, ctrl_key = self._get_id_from_header(header)
        ctrl_info = {"table_id": table_id, "start_seq": header['start_seq'], "source_id": source_id, "log_table_id": "",
                     "segment_id": segment_id, "meta_data": meta_data, "field_list": field_data}
        # Step 1: Create Log Table
        if header["aged"]:
            ctrl_info["log_table_id"] = self.adaptor.get_log_table_id(table_id, segment_id)
            if drop:
                self.adaptor.drop_table(ctrl_info["log_table_id"])
            self.adaptor.create_table(ctrl_info["log_table_id"], self.adaptor.log_table_meta, field_data, "aged")
        # Step 2: Create Data Table
        if drop:
            self.adaptor.drop_table(table_id)
        table_type = "raw" if header["aged"] else "normal"
        self.adaptor.create_table(table_id, meta_data, field_data, table_type)
        # Step 3: Initial Control information
        self.controller.key_init(ctrl_key, ctrl_info, meta_data.get("frame_length", 0))
        return True

    def _create_segment(self, header: dict, meta_data: dict, field_data: List[dict], purge: bool) -> bool:
        """Create Segment
        """
        source_id, topic_id, table_id, segment_id, ctrl_key = self._get_id_from_header(header)
        ctrl_info = {"table_id": table_id, "start_seq": header['start_seq'], "source_id": source_id, "log_table_id": "",
                     "segment_id": segment_id, "meta_data": meta_data, "field_list": field_data}
        # Step 1: Create Log Table
        if header["aged"]:
            ctrl_info["log_table_id"] = self.adaptor.get_log_table_id(table_id, segment_id)
            if purge:
                self.adaptor.drop_table(ctrl_info["log_table_id"])
            self.adaptor.create_table(ctrl_info["log_table_id"], self.adaptor.log_table_meta, field_data, "aged")
        # Step 2: Create Data Table
        if purge:
            self.adaptor.purge_segment(table_id, meta_data, meta_data.get("segment", None))
        # Step 3: Initial Control information
        self.controller.key_init(ctrl_key, ctrl_info, meta_data.get("frame_length", 0))
        return True

    def _adapte_fields(self, table_id: str, field_data: List[dict], old_field_dict: dict):
        # Field adaptation
        for field in field_data:
            # Case 1: Add field
            if field['field_name'] not in old_field_dict:
                if not self.adaptor.add_column(table_id, field):
                    self.logger.error("Add column failed", extra=self.log_context)
                    return False
            # Case 2: Adapte field (type range case: for example, c_8 to char)
            elif len(old_field_dict[field['field_name']]) > len(field['type_chain']):
                old_field = {'field_name': field['field_name'],
                             'type_chain': old_field_dict[field['field_name']]}
                if not self.adaptor.alter_column(table_id, old_field, field):
                    self.logger.error("Adapte column failed", extra=self.log_context)
                    return False
            # Case 3: Adapte field (field size)
            elif old_field_dict[field['field_name']][-1] < field['type_chain'][-1]:
                old_field = {'field_name': field['field_name'],
                             'type_chain': old_field_dict[field['field_name']]}
                if not self.adaptor.alter_column(table_id, old_field, field):
                    self.logger.error("Resize column failed", extra=self.log_context)
                    return False
        return True

    def _push_header(self, header: dict, header_data: List[dict]) -> bool:
        source_id, topic_id, table_id, segment_id, ctrl_key = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        start_seq = header["start_seq"]
        ctrl_info_dict = self.controller.get_table_info_all(table_id)
        # Case 1: Nothing existed yet, create a new table
        if not ctrl_info_dict:
            return self._create_table(header, header.get("meta_data", {}), header_data, False)
        # Case 2: Ctrl Info contains only one item and the item is itself
        if len(ctrl_info_dict) == 1 and segment_id in ctrl_info_dict:
            ctrl_info = ctrl_info_dict[segment_id]
            # Case 2.1 Old header received, do nothing
            if ctrl_info["start_seq"] >= start_seq:
                self.logger.warning("Obsoleted header received", extra=self.log_context)
                return True
            # Case 2.2 New header received, drop and recreate table
            meta_data = header.get("meta_data", {})
            meta_data.update(ctrl_info["meta_data"])
            return self._create_table(header, meta_data, header_data, True)
        # Case 3: Ctrl Info contains another segments
        else:
            ctrl_info = ctrl_info_dict.get(segment_id, {})
            # Step 1: Segment Definition Compatibility
            cur_segment_config = header.get("meta_data", {}).get("segment", None)
            cur_segment_flow = SegmentFlower(cur_segment_config)
            for id, conf in ctrl_info_dict.items():
                if id == segment_id:
                    continue
                if not cur_segment_flow.check_compatible(conf.get("meta_data", {}).get("segment", None)):
                    self.logger.error("Segment not compatible with existed ones", extra=self.log_context)
                    return False
            # Step 2: Field Level Changes
            old_field_dict = self._get_largest_field_dict(ctrl_info_dict)
            if not self._adapte_fields(table_id, header_data, old_field_dict):
                self.logger.error("Adapte table failed", extra=self.log_context)
                return False
            if ctrl_info:
                if ctrl_info["start_seq"] >= start_seq:
                    self.logger.warning("Obsoleted header received", extra=self.log_context)
                    return True
                purge = True
            else:
                purge = False
            meta_data = header.get("meta_data", {})
            meta_data.update(ctrl_info.get("meta_data", {}))
            return self._create_segment(header, meta_data, header_data, purge)

    def _std_push_data(self, header: dict, body_data: List[dict]):
        source_id, topic_id, table_id, segment_id, ctrl_key = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        ctrl_info = self.controller.get_ctrl_info(ctrl_key)
        field_data = ctrl_info.get('field_list', [])
        return self.adaptor.append_normal_data(table_id, field_data, body_data, "normal")

    def _age_push_data(self, header: dict, body_data: List[dict]):
        source_id, topic_id, table_id, segment_id, ctrl_key = self._get_id_from_header(header)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        start_age, end_age = int(header["age"]), int(header.get("end_age", header["age"]))
        ctrl_info = self.controller.get_ctrl_info(ctrl_key)
        field_data = ctrl_info.get('field_list', [])
        log_table_id = self.adaptor.get_log_table_id(table_id, segment_id)
        self.adaptor.append_log_data(log_table_id, field_data, body_data)
        load_task = self.controller.get_ready_task(ctrl_key, start_age, end_age)
        if not load_task:
            return True
        return self.load_data(**load_task)

    def push_data(self, header: dict, data: Union[List[dict], str, bytes], **kwargs) -> bool:
        data_type, data_header, data_body = self._parse_data(header, data)
        # Case 1: Header data
        if data_type == 'header':
            return self._push_header(data_header, data_body)
        # Case 2: Age insert data
        elif 'age' in header:
            return self._age_push_data(data_header, data_body)
        # Case 3: Standar insert
        else:
            return self._std_push_data(data_header, data_body)

    def get_tasks(self):
        """Get All Task of an Agent

        Should call separate load data process to input data
        """
        return self.controller.get_update_tasks()

    def load_data(self, log_table_id, table_id, field_list, meta_data, start_key, end_key):
        """Load log data into final table
        """
        return self.adaptor.load_log_data(log_table_id, table_id, field_data=field_list, meta_data=meta_data,
                                          start_age=start_key, end_age=end_key)
