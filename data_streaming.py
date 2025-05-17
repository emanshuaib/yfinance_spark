import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime

output_dir = "global_stock_data"
os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(output_dir, "all_stocks_data.csv")
indices = ['^DJI', '^IXIC', '^FTSE', '^N225', '000001.SS']

# ensure header exists
if not os.path.exists(output_file):
    pd.DataFrame(columns=['datetime','open','high','low','close','volume','ticker','fetch_time','market_status']).to_csv(output_file, index=False)

while True:
    fetch_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    raw = yf.download(tickers=indices, period='5d', interval='1m', group_by='ticker', progress=False)

    rows = []
    for tk in indices:
        try:
            one_min = raw.get(tk)
            if one_min is None or one_min.empty:
                # no 1m data at all — treat as closed
                live = False
                dt = pd.NaT
                o = h = l = c = v = None
            else:
                last = one_min.tail(1).reset_index().iloc[0]
                dt = last['Datetime']
                o, h, l, c, v = (last['Open'], last['High'], last['Low'], last['Close'], last['Volume'])
                # if Close is NaN, then market is closed
                live = not pd.isna(c)

            if not live:
                hist = yf.Ticker(tk).history(period='2d', interval='1d')
                if not hist.empty:
                    c = hist['Close'].iloc[-1]
                else:
                    c = None
                o = h = l = c
                v = None 
                if pd.isna(dt):
                    dt = pd.to_datetime(fetch_time)

            rows.append({
                'datetime':    dt,
                'open':        o,
                'high':        h,
                'low':         l,
                'close':       c,
                'volume':      v,
                'ticker':      tk,
                'fetch_time':   fetch_time,
                'market_status':    1 if live else 0
            })

        except Exception as e:
            print(f"[{fetch_time}] Error for {tk}: {e}")

    df = pd.DataFrame(rows)
    df.to_csv(
        output_file,
        mode='a',
        header=False,
        index=False
    )
    print(f"[{fetch_time}] Wrote {len(df)} rows with market status")

    time.sleep(60)