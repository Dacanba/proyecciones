import os
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Lista de símbolos principales de Binance (top 150)
binance_top150 = [
"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT", "TONUSDT", "ADAUSDT", "AVAXUSDT", "SHIBUSDT", "LINKUSDT", "DOTUSDT", "BCHUSDT", "LTCUSDT", "TRXUSDT", "ICPUSDT", "PEPEUSDT", "UNIUSDT", "NEARUSDT", "FILUSDT", "ETCUSDT", "APTUSDT", "OPUSDT", "STETHUSDT", "INJUSDT", "ARBUSDT", "HBARUSDT", "VETUSDT", "TUSDUSDT", "GRTUSDT", "MKRUSDT", "FDUSDUSDT", "QNTUSDT", "TAOUSDT", "AAVEUSDT", "MNTUSDT", "IMXUSDT", "SUIUSDT", "SANDUSDT", "XLMUSDT", "EGLDUSDT", "CROUSDT", "AXSUSDT", "CAKEUSDT", "STXUSDT", "FLOKIUSDT", "LDOUSDT", "THETAUSDT", "ALGOUSDT", "XTZUSDT", "MANAUSDT", "CHZUSDT", "KASUSDT", "MINAUSDT", "RUNEUSDT", "CRVUSDT", "SNXUSDT", "GMXUSDT", "JASMYUSDT", "DYDXUSDT", "SEIUSDT", "GALAUSDT", "WIFUSDT", "LUNCUSDT", "PYTHUSDT", "FLOWUSDT", "FETUSDT", "XECUSDT", "TWTUSDT", "KAVAUSDT", "CFXUSDT", "GLMRUSDT", "C98USDT", "ONUSDT", "ZECUSDT", "ENSUSDT", "COMPUSDT", "LPTUSDT", "ENJUSDT", "BICOUSDT", "BANDUSDT", "XVSUSDT", "MTLUSDT", "BATUSDT", "ANKRUSDT", "YGGUSDT", "SKLUSDT", "HOTUSDT", "CTSIUSDT", "1INCHUSDT", "SPELLUSDT", "GLMUSDT", "ILVUSDT", "AUDIOUSDT", "RLCUSDT", "LRCUSDT", "CELRUSDT", "COTIUSDT", "SSVUSDT", "SFPUSDT", "DUSKUSDT", "BELUSDT", "MASKUSDT", "IDUSDT", "SUSHIUSDT", "LINAUSDT", "FORTHUSDT", "CKBUSDT", "ARKMUSDT", "BOMEUSDT", "TURBOUSDT", "BBUSDT",
"ORDIUSDT", "OMUSDT", "WCTUSDT", "GUNUSDT", "USD1USDT", "SUNUSDT", "ATOMUSDT", "MEMEUSDT", "ARPAUSDT",   
]

intervalos = {
   "diario": "1d",
    "h4": "4h",
   "h1": "1h"
}

fecha_inicio = '2017-08-17'
base_path = 'data_cripto_binance'

def get_binance_klines(symbol, interval='1d', start_str='2017-08-17'):
    url = "https://api.binance.com/api/v3/klines"
    start_ts = int(pd.Timestamp(start_str).timestamp() * 1000)
    klines = []
    while True:
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_ts,
            'limit': 1000
        }
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            print(f"Rate limit: sleeping for 1s ({symbol}-{interval})")
            time.sleep(1)
            continue
        if resp.status_code != 200:
            print(f"Error en {symbol}-{interval}: {resp.status_code}")
            return None
        data = resp.json()
        if not data:
            break
        klines += data
        last_ts = data[-1][0]
        start_ts = last_ts + 1
        if len(data) < 1000:
            break
        time.sleep(0.05)  # menor delay
    if not klines:
        return None
    df = pd.DataFrame(klines, columns=[
        'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
        'Close time', 'Quote asset volume', 'Number of trades',
        'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
    ])
    # Procesar columnas de fecha y hora
    df['DateTime'] = pd.to_datetime(df['Open time'], unit='ms')
    df['date'] = df['DateTime'].dt.date.astype(str)
    df['time'] = df['DateTime'].dt.time.astype(str)
    # Reordenar y seleccionar columnas
    df_final = df[['date', 'time', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
    df_final[['Open', 'High', 'Low', 'Close', 'Volume']] = df_final[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)
    return df_final

def descarga_y_guarda(symbol, intervalo, base_path):
    df = get_binance_klines(symbol, interval=intervalo, start_str=fecha_inicio)
    if df is None or df.empty:
        print(f"Sin datos para {symbol} [{intervalo}]")
        return
    # Definir sufijo para el archivo
    if intervalo == "1d":
        file_name = f"{symbol}.csv"
    else:
        file_name = f"{symbol}_{intervalo}.csv"
    file_path = os.path.join(base_path, file_name)
    df.to_csv(file_path, index=False)
    print(f"Guardado: {file_path}")

def main_parallel(symbols, intervalos, base_path, max_workers=10):
    os.makedirs(base_path, exist_ok=True)
    tasks = []
    for nombre, intervalo in intervalos.items():
        for symbol in symbols:
            tasks.append((symbol, intervalo, base_path))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(descarga_y_guarda, *task) for task in tasks]
        for f in as_completed(futures):
            pass

if __name__ == "__main__":
    main_parallel(binance_top150, intervalos, base_path, max_workers=10)