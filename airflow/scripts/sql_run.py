from sqlalchemy import create_engine


def sql_run(post_string, sql, **kwargs):
    try:
        engine = create_engine(post_string)
        post_conn = engine.connect()
        data = post_conn.execute(sql)
    except Exception as e:
        return e
    return [dict(d) for d in data]

    try:
        engine = create_engine(post_string)
        post_conn = engine.connect()
    except Exception as e:
        return e
