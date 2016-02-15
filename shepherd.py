__author__ = 'alessio.rocchi'

from core.watcher import Watcher2, Resolver
from core.reactioneer import Reactioneer
from core.executor import Executor
from core.guard import Guard as Guardian
from threading import Event, Thread
from daemonize import Daemonize
from argparse import ArgumentParser
from rofl import text
import ConfigParser
import Queue
import sys

import logging

pid = "/var/run/shep.pid"

logFormatter = logging.Formatter("%(asctime)s [%(name)-32.32s] [%(threadName)-11.11s] [%(levelname)-7.7s]  %(message)s")
rootLogger = logging.getLogger('shepherd')
rootLogger.setLevel(logging.DEBUG)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

fileHandler = logging.FileHandler("/var/log/shep.log", "w")
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

keep_fds = [fileHandler.stream.fileno()]

reaction_queue = Queue.Queue()
executor_queue = Queue.Queue()

Config = ConfigParser.ConfigParser()
Config.read('config.ini')

def config_section_map(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
        except KeyError:
            dict1[option] = None
    return dict1


class Supervisor(Thread):
    def __init__(self, event):
        super(Supervisor, self).__init__()
        self.name = "Supervisor"
        self.stop = event
        self.logger = logging.getLogger('shepherd.Supervisor')


def main():
    rootLogger.info(text)
    rabbitmq_config = config_section_map("rabbitmq")
    vcloud_config = config_section_map("vcloud")
    vcenter_config = config_section_map("vcenter")
    executor_config = config_section_map("executor")

    watch = Watcher2(rabbitmq=rabbitmq_config['host'],
                     username=rabbitmq_config['username'],
                     password=rabbitmq_config['password'])

    resolver = Resolver(host=vcloud_config['vcloud'],
                        username=vcloud_config['username'],
                        password=vcloud_config['password'],
                        reaction_queue=reaction_queue)

    reactioneer = Reactioneer(reaction_queue, executor_queue, dispatch_any=rabbitmq_config['dispatch_any'])

    executor = Executor(host=vcenter_config['vcenter'],
                        username=vcenter_config['username'],
                        password=vcenter_config['password'],
                        executor_queue=executor_queue,
                        create_affinity_rule=executor_config['create_affinity'])

    guardian_event = Event()
    guardian = Guardian(host=vcenter_config['vcenter'],
                        username=vcenter_config['username'],
                        password=vcenter_config['password'],
                        event=guardian_event)

    resolver.start()
    watch.start()
    reactioneer.start()
    executor.start()
    guardian.start()

    try:
        resolver.join()
        watch.join()
        reactioneer.join()
        executor.join()
        guardian.join()
    except KeyboardInterrupt:
        rootLogger.info("Caught CTRL+C. Waiting for all threads to finish...")
        resolver.stop = True
        watch.stop = True
        reactioneer.stop = True
        executor.stop = True
        guardian.stopped.set()


if __name__ == '__main__':
    parser = ArgumentParser(prog='shepherd', add_help=True)
    parser.add_argument('--daemon', action='store_true', dest='daemon', help='Run the program as Daemon')
    parser.add_argument('--name', type=str, help='Vcloud Cell Name. This is only needed to identify the process',
                        required=True)
    p = parser.parse_args()
    if p.daemon is True:
        daemon = Daemonize(app="shepherd", pid=pid, action=main, keep_fds=keep_fds)
        daemon.start()
    else:
        main()
