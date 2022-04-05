### token taken from instruent table 

from kiteconnect import KiteTicker
import logging
import logging.handlers
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:ncr*123@localhost/papahere")
conn=engine.connect()
#update configurations
configdata=pd.read_sql(''' select * from kite_prod.config_app 
                        where configname != 'websocket_tokens' ''',con=conn)
for i,j in zip(configdata.configname, configdata.configvalue):
    print(i)
    locals().update({i:j})
dropcols={'ohlc_open', 'ohlc_high', 'ohlc_low',
                            'ohlc_close', 'oi_day_high', 'oi_day_low'}
symbol = pd.read_sql(f'''
select tradingsymbol,instrument_token,mode_stream from kite_prod.target_scripts;
''',con=engine)#reading table for instrument to trading symbol mapping 
def depth_summary (record,prefix='buy'):
    record = pd.DataFrame(record)
    out = {}
    out[f'{prefix}_bidnos'] = record.quantity.sum()
    out[f'{prefix}_bid']= sum(record.price*record.quantity)/out[f'{prefix}_bidnos']
    out[f'{prefix}_orders'] = record.orders.sum()
    return out
def enhancetick(line):
       df= pd.json_normalize(line,sep='_')
       df=df.merge(symbol[['tradingsymbol','instrument_token']],on='instrument_token',how='left')
       if 'depth_buy' in df.columns:
              a  = df.pop('depth_buy') 
              a.dropna(inplace=True)
              a=a.apply(depth_summary).apply(pd.Series)
              df= df.join(a)
       if 'depth_sell' in df.columns:
              b  = df.pop('depth_sell') 
              b.dropna(inplace=True)
              b=b.apply(depth_summary,args=('sell',)).apply(pd.Series)
              df=df.join(b)
       df.drop(columns=list(dropcols.intersection(df.columns)),inplace=True)
       return df

def tick_to_postgre(ticks):
    p=enhancetick(line=ticks)
    p=p.to_sql(name='ticks',con=conn,schema='kite_prod',if_exists='append',index=False)

LOG_FILENAME = f'F:/websocketlogs/logging_{session_date}.out'
my_logger = logging.getLogger('MyLogger')
my_logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=50000000, backupCount=1000)
my_logger.addHandler(handler)
kws=KiteTicker(api_key=apikey,access_token=access_token,reconnect=True,reconnect_max_tries=50,reconnect_max_delay=50)
###############################bankniftyinstr = eval(websocket_tokens)

def on_ticks(ws,ticks):
    my_logger.debug(ticks)
    tick_to_postgre(ticks)
def on_connect(ws,response):
    modes={'full':ws.MODE_FULL,'ltp':ws.MODE_LTP,'quote':ws.MODE_QUOTE}
    ws.subscribe(symbol.instrument_token.tolist())
    for i in symbol.mode_stream.unique():
        ws.set_mode(modes[i], symbol[symbol.mode_stream==i].instrument_token.tolist())
def on_order_update(ws, data):
    my_logger.debug("on_order_update: {}".format(data))
    try:
        data = pd.json_normalize(data,sep='_')
        p=p.to_sql(name='order_updates',con=conn,schema='kite_prod',if_exists='append',index=False)
    except:
        print(data)
kws.on_ticks=on_ticks
kws.on_connect=on_connect
kws.on_order_update=on_order_update
kws.connect(disable_ssl_verification=True,threaded=False)
