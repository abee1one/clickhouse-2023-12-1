import requests
import pandas as pd
from io import StringIO


HOST = 'http://127.0.0.1:8123'

query_ddl = '''
create table test(a UInt8, b UInt8, c String) Engine MergeTree ORDER BY a;
'''

query_insert = '''
insert into test(a,b,c) VALUES (1,2,'user_1') (1, 2, 'user_3') (1, 2, 'user_5')  (1, 3, 'user_1')  (1, 3, 'user_5') 
'''

query_select = '''
select * from test where c = 'user_5'
'''


def query(q, host=HOST, conn_timeout=1500, **kwargs):
    r = requests.post(host, params=kwargs, timeout=conn_timeout, data=q)
    print(r.text)
    if r.status_code == 200:
        return r.text
    else:
        raise ValueError(r.text)

if __name__ == '__main__':
    # query(query_ddl)
    # query(query_insert)
    data = query(query_select)
    dataframe = pd.read_csv(StringIO(data), sep='\t', names=['a', 'b', 'c'])
    print(dataframe.head())
    print(dataframe.shape)