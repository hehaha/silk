import logging
import traceback

from django.db.models.sql import EmptyResultSet
from django.utils import timezone

from silk.collector import DataCollector
from silk.config import SilkyConfig

Logger = logging.getLogger('silk.sql')


def _should_wrap(sql_query):
    for ignore_str in SilkyConfig().SILKY_IGNORE_QUERIES:
        if ignore_str in sql_query:
            return False
    return True


def execute_sql(self, *args, **kwargs):
    """wrapper around real execute_sql in order to extract information"""

    try:
        q, params = self.as_sql()
        if not q:
            raise EmptyResultSet
    except EmptyResultSet:
        try:
            result_type = args[0]
        except IndexError:
            result_type = kwargs.get('result_type', 'multi')
        if result_type == 'multi':
            return iter([])
        else:
            return
    tb = ''.join(reversed(traceback.format_stack()))
    sql_query = q % params
    if _should_wrap(sql_query):
        query_dict = {
            'query': sql_query,
            'start_time': timezone.now(),
            'traceback': tb
        }
        try:
            return self._execute_sql(*args, **kwargs)
        finally:
            query_dict['end_time'] = timezone.now()
            request = DataCollector().request
            if request:
                query_dict['request'] = request
            if self.query.model.__module__ != 'silk.models':
                DataCollector().register_query(query_dict)
            else:
                DataCollector().register_silk_query(query_dict)
    return self._execute_sql(*args, **kwargs)


def execute_insertsql(self, *args, **kwargs):
    sql_queries = [q % params for q, params in self.as_sql()]
    if all([_should_wrap(query) for query in sql_queries]):
        tb = ''.join(reversed(traceback.format_stack()))
        start = timezone.now()
        query_dict_list = [
            {"query": query, "start_time": start, 'traceback': tb} for query in sql_queries]
        try:
            return self._execute_sql(*args, **kwargs)
        finally:
            end = timezone.now()
            collector = DataCollector()
            request = collector.request
            for query_dict in query_dict_list:
                query_dict['end_time'] = end
                if request:
                    query_dict['request'] = request
                if self.query.model.__module__ != 'silk.models':
                    collector.register_query(query_dict)
                else:
                    collector.register_silk_query(query_dict)
    return self._execute_sql(*args, **kwargs)
