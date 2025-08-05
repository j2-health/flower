import datetime
import time

from .search import parse_search_terms, satisfies_search_terms


def extract_tenant_from_headers(task):
    """
    Extract tenant information from task headers.
    Looks for _schema value in the headers.
    """
    try:
        # Check if task has headers attribute
        if hasattr(task, 'headers') and task.headers:
            return task.headers.get('_schema', '')
        
        # Check if task has request attribute with headers
        if hasattr(task, 'request') and hasattr(task.request, 'headers'):
            return task.request.headers.get('_schema', '')
        
        # Check if task has kwargs that might contain headers
        if hasattr(task, 'kwargs') and task.kwargs:
            if isinstance(task.kwargs, dict):
                return task.kwargs.get('_schema', '')
            elif isinstance(task.kwargs, str):
                # Try to parse kwargs string for _schema
                import ast
                try:
                    kwargs_dict = ast.literal_eval(task.kwargs)
                    if isinstance(kwargs_dict, dict):
                        return kwargs_dict.get('_schema', '')
                except (ValueError, SyntaxError):
                    pass
        
        return ''
    except Exception:
        return ''


# pylint: disable=too-many-branches,too-many-locals,too-many-arguments
def iter_tasks(events, limit=None, offset=0, type=None, worker=None, state=None,
               sort_by=None, received_start=None, received_end=None,
               started_start=None, started_end=None, search=None):
    i = 0
    tasks = events.state.tasks_by_timestamp()
    if sort_by is not None:
        tasks = sort_tasks(tasks, sort_by)

    def convert(x):
        return time.mktime(datetime.datetime.strptime(x, '%Y-%m-%d %H:%M').timetuple())

    search_terms = parse_search_terms(search or {})

    for uuid, task in tasks:
        if type and task.name != type:
            continue
        if worker and task.worker and task.worker.hostname != worker:
            continue
        if state and task.state != state:
            continue
        if received_start and task.received and\
                task.received < convert(received_start):
            continue
        if received_end and task.received and\
                task.received > convert(received_end):
            continue
        if started_start and task.started and\
                task.started < convert(started_start):
            continue
        if started_end and task.started and\
                task.started > convert(started_end):
            continue
        if not satisfies_search_terms(task, search_terms):
            continue
        if i >= offset:
            yield uuid, task
        i += 1
        if limit is not None:
            if i == limit + offset:
                break


sort_keys = {'name': str, 'state': str, 'tenant': str, 'received': float, 'started': float}


def sort_tasks(tasks, sort_by):
    assert sort_by.lstrip('-') in sort_keys
    reverse = False
    if sort_by.startswith('-'):
        sort_by = sort_by.lstrip('-')
        reverse = True
    yield from sorted(
            tasks,
            key=lambda x: getattr(x[1], sort_by) or sort_keys[sort_by](),
            reverse=reverse)


def get_task_by_id(events, task_id):
    return events.state.tasks.get(task_id)


def as_dict(task):
    task_dict = task.as_dict()
    # Add tenant information extracted from headers
    task_dict['tenant'] = extract_tenant_from_headers(task)
    return task_dict
