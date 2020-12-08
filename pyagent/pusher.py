from typing import Dict, List, Union, Tuple
from xialib.adaptor import Adaptor
from xialib.storer import Storer
from xialib.subscriber import Subscriber
from pyagent.agent import Agent

__all__ = ['Pusher']

class Pusher(Agent):
    """Pusher Agent
    Push received data without flow control. Receive header = drop and create new table

    Attributes:
        sources (:obj:`list` of `Subscriber`): Data sources

    """
    def __init__(self,
                 storers: List[Storer],
                 adaptor_dict: Dict[str, Adaptor],
                 **kwargs):
        super().__init__(storers=storers, adaptor_dict=adaptor_dict, **kwargs)
        self.field_data_dict = dict()

    def push_data(self, header: dict, data: Union[List[dict], str, bytes], **kwargs) -> bool:
        topic_id = header['topic_id']
        table_id = header['table_id']
        self.log_context['context'] = '-'.join([topic_id, table_id])

        target_id = '.'.join(table_id.split('.')[:2])
        active_adaptor = self.adaptor_dict.get(target_id, None)
        assert isinstance(active_adaptor, Adaptor)
        if active_adaptor is None:
            self.logger.error("No adaptor for target id {}".format(target_id), extra=self.log_context)
            raise ValueError("AGT-000005")

        data_type, data_header, data_body = self._parse_data(header, data)
        if data_type == 'header':
            active_adaptor.drop_table(table_id)
            self.field_data_dict[table_id] = data_body
            return active_adaptor.create_table(table_id, header.get('meta-data', dict()), data_body)
        else:
            field_data = self.field_data_dict.get(table_id, None)
            if field_data is None:
                line = active_adaptor.get_ctrl_info(table_id)
                if not line.get('FIELD_LIST', None):
                    self.logger.warning("Field list empty or not found {}".format(table_id), extra=self.log_context)
                    return False
                field_data = line['FIELD_LIST']
            return active_adaptor.upsert_data(table_id, field_data, data_body, True)
