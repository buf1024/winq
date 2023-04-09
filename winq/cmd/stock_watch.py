import asyncio
import click
import os
from winq import log
import hiq_pyfetch as fetch
from datetime import datetime

from winq.common import run_until_complete, setup_log
from winq.config import init_def_config
from winq.email_util import EmailUtil


class StockWatch:
    def __init__(self, watch_path: str, task_count: int, interval: int, falling: float, notify_file: str, email_sender: EmailUtil = None, email_receiver: str = '') -> None:
        self.log = log.get_logger(self.__class__.__name__)
        self.watch_path = watch_path
        self.interval = interval
        self.falling = falling
        self.notify_file = notify_file
        self.email_sender = email_sender
        self.email_receiver = email_receiver

        self.task_count = task_count

        self.watch_codes = []
        self.notify_codes = []

    def fill_watch(self):
        codes = ''
        with open(self.watch_path) as f:
            codes = f.read()

        self.watch_codes = []
        if len(codes) > 0:
            codes = codes.split(',')
            self.watch_codes = list(
                filter(lambda code: code not in self.notify_codes, codes))

    def prepare(self) -> bool:
        if not os.path.isfile(self.watch_path):
            self.log.error('path: {} is not a file'.format(self.watch_path))
            return False

        if self.notify_file is not None:
            try:
                with open(self.notify_file, 'a') as f:
                    pass
            except Exception as e:
                self.log.error('result path exception: {}'.format(e))
                return False

        if self.interval < 0:
            self.log.error(
                'sleep interval: {} is not valid'.format(self.interval))
            return False

        if self.task_count < 0:
            self.log.error(
                'task count: {} is not valid'.format(self.task_count))
            return False

        self.fill_watch()
        if len(self.watch_codes) <= 0:
            self.log.error('watch code is empty!')
            return False

        return True

    async def watch_task(self, codes):
        n = datetime.now()
        for code in codes:
            data = await fetch.fetch_stock_bar(code=code, skip_rt=False, start=n, end=n)
            kdata = data['bars']
            if kdata is not None and len(kdata) > 0:
                data0 = kdata.iloc[0]
                last_close, high, chg_pct = data0['close'] / \
                    (1.0 + data0['chg_pct'] /
                     100), data0['high'],  data0['chg_pct']

                high_chg_pct = (high - last_close) * 100 / last_close

                falling = high_chg_pct - chg_pct
                if falling >= self.falling or chg_pct < 0:
                    await self.notify_task(code=code, high=high_chg_pct, now=chg_pct, falling=falling)

    async def notify_task(self, code, high, now, falling):
        self.notify_codes.append(code)
        cont = 'code: {}, high: {}{}%, now: {}{}%, falling: {}% time: {}'.format(
            code,
            '+' if high > 0 else '', round(high, 2),
            '+' if now > 0 else '', round(now, 2),
            round(falling, 2),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        self.log.info('{}, fit the condition: falling({}%) or chg_pct < 0, get notified!'
                      .format(cont, self.falling))

        if self.notify_file is not None:
            self.log.info('file get notified')
            with open(self.notify_file, 'a') as f:
                f.write(cont + '\n')

        if self.email_sender is not None and len(self.email_receiver) > 0:
            self.log.info('email get notified')
            title = '{} fit sell signal!'.format(code)
            await self.email_sender.send_email(email=self.email_receiver, title=title, content=cont)

    async def watch(self):
        while True:
            self.fill_watch()
            size = len(self.watch_codes)
            if size > 0:
                self.log.info('start watch: {}'.format(self.watch_codes))
                s, r = int(size / self.task_count), size % self.task_count
                tasks = []
                if s > 0:
                    for i in range(s):
                        start = i*self.task_count
                        end = start + self.task_count if i != s - 1 else start+self.task_count + r
                        tasks.append(self.watch_task(
                            codes=self.watch_codes[start:end][:]))
                else:
                    tasks.append(self.watch_task(codes=self.watch_codes[:]))

                await asyncio.gather(*tasks)

            await asyncio.sleep(self.interval)


@click.command()
@click.option('--watch-path', type=str, default='', help='watch codes path')
@click.option('--task-count', type=int, default=10, help='watch task count, default: 10')
@click.option('--interval', default=10, type=int, help='sleep interval, default: 10')
@click.option('--falling', default=1.5, type=float, help='max falling down, default: 1.5')
@click.option('--notify-file', default='notify-file.txt', type=str, help='notify result path, default: notify-file.txt')
@click.option('--sender-email', default='winnerquant@163.com', type=str, help='notify sender email, default: None')
@click.option('--sender-host', default='smtp.163.com:465', type=str, help='notify sender host, default: None')
@click.option('--sender-key', default='VMOXNIYXLYHWUSKP', type=str, help='notify sender smtp key, default: None')
@click.option('--receiver-email', default='450171094@qq.com', type=str, help='notify receiver email, default: None')
def main(watch_path: str, task_count: int, interval: int,
         falling: float, notify_file: str,
         sender_email: str, sender_host: str, sender_key: str, receiver_email: str):
    
    if len(watch_path) == 0 or not os.path.isfile(watch_path) or not os.path.exists(watch_path):
        print('watch_path: {} is not valid'.format(watch_path))
        return None
    
    host_port = sender_host.split(':')
    if len(host_port) != 2:
        print('smtp: {} is not valid'.format(sender_host))
        return None
    
    email_sender = EmailUtil(user=sender_email, passwd=sender_key,
                      smtp_host=host_port[0], smtp_port=int(host_port[1]))

    _, conf_dict = init_def_config()
    setup_log(conf_dict, 'watch.log')

    w = StockWatch(watch_path=watch_path, task_count=task_count,
                   interval=interval, falling=falling,
                   notify_file=notify_file,
                   email_sender=email_sender,
                   email_receiver=receiver_email)

    if not w.prepare():
        print('prepare error!')
        return False

    run_until_complete(w.watch())


if __name__ == '__main__':
    main()
