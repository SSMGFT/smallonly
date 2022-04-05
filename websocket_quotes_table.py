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

symbol = pd.read_sql(f'''
select tradingsymbol,instrument_token,mode_stream from kite_prod.target_scripts;
''',con=engine)#reading table for instrument to trading symbol mapping 

def tick_to_postgre(ticks):
    p=pd.json_normalize(ticks,sep='_')
    try:
        p.drop(columns=['depth_buy','depth_sell'],inplace=True)
    except:
        pass
    ## adding trading symbol
    p=p.merge(symbol[['tradingsymbol','instrument_token']],on='instrument_token',how='left')
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
    print(data)

kws.on_ticks=on_ticks
kws.on_connect=on_connect
kws.on_order_update=on_order_update
kws.connect(disable_ssl_verification=True,threaded=False)
