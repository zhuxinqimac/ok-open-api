import os
import argparse
import asyncio
import websockets
import json
import requests
import dateutil.parser as dp
import hmac
import base64
import zlib
import logging
import datetime

log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='mylog-ws.json', filemode='w', format=log_format, level=logging.INFO)

# logging.warning('warn message')
logging.info('info message')
# logging.debug('debug message')
# logging.error('error message')
# logging.critical('critical message')


def get_timestamp():
    now = datetime.datetime.now()
    t = now.isoformat("T", "milliseconds")
    return t + "Z"


def get_server_time():
    url = "https://www.okex.com/api/general/v3/time"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['iso']
    else:
        return ""


def server_timestamp():
    server_time = get_server_time()
    parsed_t = dp.parse(server_time)
    timestamp = parsed_t.timestamp()
    return timestamp


def login_params(timestamp, api_key, passphrase, secret_key):
    message = timestamp + 'GET' + '/users/self/verify'

    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    sign = base64.b64encode(d)

    login_param = {"op": "login", "args": [api_key, passphrase, timestamp, sign.decode("utf-8")]}
    login_str = json.dumps(login_param)
    return login_str


def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


def partial(res, timestamp, filename, args):
    data_obj = res['data'][0]
    bids = data_obj['bids']
    asks = data_obj['asks']
    with open(filename, 'a') as f:
        f.write(timestamp + 'bids:' + str(bids[:args.keep_depth_size]) + '\n')
        f.write('n_bids:' + str(len(bids[:args.keep_depth_size])) + '\n')
    print(timestamp + '全量数据bids为：' + str(bids[:args.keep_depth_size]))
    print('档数为：' + str(len(bids[:args.keep_depth_size])))
    with open(filename, 'a') as f:
        f.write(timestamp + 'asks:' + str(asks[:args.keep_depth_size]) + '\n')
        f.write('n_asks:' + str(len(asks[:args.keep_depth_size])) + '\n')
    print(timestamp + '全量数据asks为：' + str(asks[:args.keep_depth_size]))
    print('档数为：' + str(len(asks[:args.keep_depth_size])))
    return bids, asks


def update_bids(res, bids_p, timestamp, filename, args):
    # 获取增量bids数据
    bids_u = res['data'][0]['bids']
    print(timestamp + '增量数据bids为：' + str(bids_u))
    print('档数为：' + str(len(bids_u)))
    # bids合并
    for i in bids_u:
        bid_price = i[0]
        for j in bids_p:
            if bid_price == j[0]:
                if i[1] == '0':
                    bids_p.remove(j)
                    break
                else:
                    del j[1]
                    j.insert(1, i[1])
                    break
        else:
            if i[1] != "0":
                bids_p.append(i)
    else:
        bids_p.sort(key=lambda price: sort_num(price[0]), reverse=True)
        with open(filename, 'a') as f:
            f.write(timestamp + 'bids:' + str(bids_p[:args.keep_depth_size]) + '\n')
            f.write('n_bids:' + str(len(bids_p[:args.keep_depth_size])) + '\n')
        print(timestamp + '合并后的bids为：' + str(bids_p[:args.keep_depth_size]) + 
                '，档数为：' + str(len(bids_p[:args.keep_depth_size])))
        # logging.info('combine bids:' + str(bids_p))
    return bids_p


def update_asks(res, asks_p, timestamp, filename, args):
    # 获取增量asks数据
    asks_u = res['data'][0]['asks']
    print(timestamp + '增量数据asks为：' + str(asks_u))
    print('档数为：' + str(len(asks_u)))
    # asks合并
    for i in asks_u:
        ask_price = i[0]
        for j in asks_p:
            if ask_price == j[0]:
                if i[1] == '0':
                    asks_p.remove(j)
                    break
                else:
                    del j[1]
                    j.insert(1, i[1])
                    break
        else:
            if i[1] != "0":
                asks_p.append(i)
    else:
        asks_p.sort(key=lambda price: sort_num(price[0]))
        with open(filename, 'a') as f:
            f.write(timestamp + 'asks:' + str(asks_p[:args.keep_depth_size]) + '\n')
            f.write('n_asks:' + str(len(asks_p[:args.keep_depth_size])) + '\n')
        print(timestamp + '合并后的asks为：' + str(asks_p[:args.keep_depth_size]) + 
                '，档数为：' + str(len(asks_p[:args.keep_depth_size])))
        # logging.info('combine asks:' + str(asks_p))
    return asks_p


def sort_num(n):
    if n.isdigit():
        return int(n)
    else:
        return float(n)


def check(bids, asks):
    if len(bids) >= 25 and len(asks) >= 25:
        bids_l = []
        asks_l = []
        for i in range(1, 26):
            bids_l.append(bids[i - 1])
            asks_l.append(asks[i - 1])
        bid_l = []
        ask_l = []
        for j in bids_l:
            str_bid = ':'.join(j[0 : 2])
            bid_l.append(str_bid)
        for k in asks_l:
            str_ask = ':'.join(k[0 : 2])
            ask_l.append(str_ask)
        num = ''
        for m in range(len(bid_l)):
            num += bid_l[m] + ':' + ask_l[m] + ':'
        new_num = num[:-1]
        int_checksum = zlib.crc32(new_num.encode())
        fina = change(int_checksum)
        return fina

    else:
        logging.error("depth data < 25 is error")
        print('深度数据少于25档')


def change(num_old):
    num = pow(2, 31) - 1
    if num_old > num:
        out = num_old - num * 2 - 2
    else:
        out = num_old
    return out


# subscribe channels un_need login
async def subscribe_without_login(args, url, channels, filename):
    while True:
        try:
            async with websockets.connect(url) as ws:
                sub_param = {"op": "subscribe", "args": channels}
                sub_str = json.dumps(sub_param)
                await ws.send(sub_str)
                # logging.info(f"send: {sub_str}")

                while True:
                    try:
                        res_b = await asyncio.wait_for(ws.recv(), timeout=25)
                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
                        # logging.error(e)
                        try:
                            await ws.send('ping')
                            res_b = await ws.recv()
                            res = inflate(res_b).decode('utf-8')
                            print(res)
                            # logging.info(res)
                            continue
                        except Exception as e:
                            print("连接关闭，正在重连……")
                            # logging.error(e)
                            break
                    if args.type.startswith('candle'):
                        await asyncio.sleep(int(args.type[6:-1]) // 10)
                    else:
                        await asyncio.sleep(2)

                    res = inflate(res_b).decode('utf-8')
                    # logging.info(f"recv: {res}")
                    timestamp = get_timestamp()
                    with open(filename, 'a') as f:
                        f.write(timestamp + res + '\n')
                    print(timestamp + res)
                    res = eval(res)
                    if 'event' in res:
                        continue

                    for i in res:
                        if 'depth' in res[i] and 'depth5' not in res[i]:
                            # 订阅频道是深度频道
                            if res['action'] == 'partial':
                                # 获取首次全量深度数据
                                bids_p, asks_p = partial(res, timestamp, filename, args)

                                # 校验checksum
                                checksum = res['data'][0]['checksum']
                                print(timestamp + '推送数据的checksum为：' + str(checksum))
                                # logging.info('get checksum:' + str(checksum))
                                check_num = check(bids_p, asks_p)
                                print(timestamp + '校验后的checksum为：' + str(check_num))
                                # logging.info('calculate checksum:' + str(check_num))
                                if check_num == checksum:
                                    with open(filename, 'a') as f:
                                        f.write('checksum: True\n')
                                    print("校验结果为：True")
                                    # logging.info('checksum: True')
                                else:
                                    with open(filename, 'a') as f:
                                        f.write('checksum: False\n')
                                    print(timestamp + "校验结果为：False，正在重新订阅……")
                                    # logging.error('checksum: False')

                                    # 取消订阅
                                    await unsubscribe_without_login(url, channels, timestamp)
                                    # 发送订阅
                                    async with websockets.connect(url) as ws:
                                        sub_param = {"op": "subscribe", "args": channels}
                                        sub_str = json.dumps(sub_param)
                                        await ws.send(sub_str)
                                        print(timestamp + f"send: {sub_str}")
                                        # logging.info(f"send: {sub_str}")

                            elif res['action'] == 'update':
                                # 获取合并后数据
                                bids_p = update_bids(res, bids_p, timestamp, filename, args)
                                asks_p = update_asks(res, asks_p, timestamp, filename, args)

                                # 校验checksum
                                checksum = res['data'][0]['checksum']
                                print(timestamp + '推送数据的checksum为：' + str(checksum))
                                # logging.info('get checksum:' + str(checksum))
                                check_num = check(bids_p, asks_p)
                                print(timestamp + '校验后的checksum为：' + str(check_num))
                                # logging.info('calculate checksum:' + str(check_num))
                                if check_num == checksum:
                                    print("校验结果为：True")
                                    # logging.info('checksum: True')
                                else:
                                    print(timestamp + "校验结果为：False，正在重新订阅……")
                                    # logging.error('checksum: False')

                                    # 取消订阅
                                    await unsubscribe_without_login(url, channels, timestamp)
                                    # 发送订阅
                                    async with websockets.connect(url) as ws:
                                        sub_param = {"op": "subscribe", "args": channels}
                                        sub_str = json.dumps(sub_param)
                                        await ws.send(sub_str)
                                        print(timestamp + f"send: {sub_str}")
                                        # logging.info(f"send: {sub_str}")
        except Exception as e:
            # logging.info(e)
            print("连接断开，正在重连……")
            continue


# subscribe channels need login
async def subscribe(url, api_key, passphrase, secret_key, channels):
    while True:
        try:
            async with websockets.connect(url) as ws:
                # login
                timestamp = str(server_timestamp())
                login_str = login_params(timestamp, api_key, passphrase, secret_key)
                await ws.send(login_str)
                time = get_timestamp()
                print(time + f"send: {login_str}")
                logging.info(f"send: {login_str}")
                res_b = await ws.recv()
                res = inflate(res_b).decode('utf-8')
                time = get_timestamp()
                print(time + res)
                logging.info(f"recv: {res}")
                await asyncio.sleep(1)

                # subscribe
                sub_param = {"op": "subscribe", "args": channels}
                sub_str = json.dumps(sub_param)
                await ws.send(sub_str)
                time = get_timestamp()
                print(time + f"send: {sub_str}")
                logging.info(f"send: {sub_str}")

                while True:
                    try:
                        res_b = await asyncio.wait_for(ws.recv(), timeout=25)
                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
                        # logging.error(e)
                        try:
                            await ws.send('ping')
                            res_b = await ws.recv()
                            res = inflate(res_b).decode('utf-8')
                            print(res)
                            logging.info(res)
                            continue
                        except Exception as e:
                            print("连接关闭，正在重连……")
                            logging.error(e)
                            break

                    res = inflate(res_b).decode('utf-8')
                    time = get_timestamp()
                    print(time + res)
                    logging.info(f"recv: {res}")

        except Exception as e:
            logging.info(e)
            print("连接断开，正在重连……")
            continue


# unsubscribe channels
async def unsubscribe(url, api_key, passphrase, secret_key, channels):
    async with websockets.connect(url) as ws:
        # login
        timestamp = str(server_timestamp())
        login_str = login_params(str(timestamp), api_key, passphrase, secret_key)
        await ws.send(login_str)
        time = get_timestamp()
        print(time + f"send: {login_str}")
        logging.info(f"send: {login_str}")

        res_1 = await ws.recv()
        res = inflate(res_1).decode('utf-8')
        time = get_timestamp()
        print(time + res)
        logging.info(f"recv: {res}")
        await asyncio.sleep(1)

        # unsubscribe
        sub_param = {"op": "unsubscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await ws.send(sub_str)
        time = get_timestamp()
        print(time + f"send: {sub_str}")
        logging.info(f"send: {sub_str}")

        res_1 = await ws.recv()
        res = inflate(res_1).decode('utf-8')
        time = get_timestamp()
        print(time + res)
        logging.info(f"recv: {res}")


# unsubscribe channels
async def unsubscribe_without_login(url, channels, timestamp):
    async with websockets.connect(url) as ws:
        # unsubscribe
        sub_param = {"op": "unsubscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await ws.send(sub_str)
        print(timestamp + f"send: {sub_str}")
        logging.info(f"send: {sub_str}")

        res_1 = await ws.recv()
        res = inflate(res_1).decode('utf-8')
        print(timestamp + f"recv: {res}")
        logging.info(f"recv: {res}")


if __name__ == '__main__':
    api_key = ""
    seceret_key = ""
    passphrase = ""

    url = 'wss://real.okex.com:8443/ws/v3'

    parser = argparse.ArgumentParser(description='Websocket collect data.')
    parser.add_argument('type', type=str, 
            choices=['ticker', 'candle', 'trade', 'depth'],  
            help='What data to collect.')
    parser.add_argument('instrument_id', type=str, default='ETH-BTC', 
            help='What currecy pair to collect.')
    parser.add_argument('--results_dir', type=str, default='./spot_results', 
            help='Results directory to save data.')
    parser.add_argument('--candle_time', type=str, default='60s', 
            # 60/180/300/900/1800/3600/7200/14400/21600/43200/86400/604800
            help='Candle time.')
    parser.add_argument('--keep_depth_size', type=int, default=50, 
            help='Size kept of depth data.')

    # channels = []
    # 用户币币账户频道
    # channels = ["spot/account:XRP"]
    # 用户杠杆账户频道
    # channels = ["spot/margin_account:BTC-USDT"]
    # 用户交易频道
    # channels = ["spot/order:XRP-USDT"]
    # 公共-Ticker频道
    args = parser.parse_args()
    if args.type == 'candle':
        args.type = args.type + args.candle_time
    quest = 'spot/' + args.type + ':' + args.instrument_id

    if not os.path.exists(args.results_dir):
        os.makedirs(args.results_dir)

    if args.type == 'depth':
        args.type = args.type + str(args.keep_depth_size)
    filename = args.type + '-' + args.instrument_id + '.txt'
    filename = os.path.join(args.results_dir, filename)

    channel = [quest]

    loop = asyncio.get_event_loop()
    # public data
    loop.run_until_complete(subscribe_without_login(args, url, channel, filename))

    # personal data
    # loop.run_until_complete(subscribe(url, api_key, passphrase, seceret_key, channels))

    loop.close()
