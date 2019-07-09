import json

from airflow.hooks.base_hook import BaseHook


def get_extra_from_conn(conn_id):
    """
    Obtain extra fields from airflow connection.

    Parameters
    ----------
    conn_id : str
        Airflow Connection ID

    Returns
    -------
    dict
        extra kwargs
    """
    hook = BaseHook(conn_id)
    conn = hook.get_connection(conn_id)
    return json.loads(conn.extra)
