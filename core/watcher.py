__author__ = 'alessio.rocchi'

import pika
import Queue
import logging
import time
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import ParseError
from threading import Thread
from vcloudlib import vcloudsession

module_logger = logging.getLogger('shepherd.watcher')
resolver_queue = Queue.Queue()


class Resolver(Thread):
    def __init__(self, host, username, password, reaction_queue):
        super(Resolver, self).__init__()
        self.host = host
        self.username = username
        self.password = password
        self.vcs = vcloudsession.VCS(host=self.host, username=self.username, password=self.password)
        self.stop = False
        self.name = 'Resolver'
        self.reaction_queue = reaction_queue
        self.logger = logging.getLogger('shepherd.watcher.Resolver')
        self.logger.info("Resolver initialized. Waiting for events...")

    def login(self):
        self.vcs.login()

    def run(self):
        while not self.stop:
            try:
                entity = resolver_queue.get(timeout=10)
            except Queue.Empty:
                continue
            self.login()
            self.logger.debug('Received Entity: {entity}'.format(entity=entity))
            response = self.vcs.execute_request(
                'http://{host}/api/entity/{urn}'.format(host=self.host, urn=entity)
            )
            if hasattr(response, 'content'):
                res_xml = ET.fromstring(response.content)
                obj_response = self.vcs.execute_request(
                    res_xml.findall('{http://www.vmware.com/vcloud/v1.5}Link')[0].attrib['href']
                )
                if obj_response:
                    vm_mo_ref = ET.fromstring(obj_response.content).findall(
                        '{http://www.vmware.com/vcloud/v1.5}VCloudExtension'
                    )[0].find(
                        '{http://www.vmware.com/vcloud/extension/v1.5}VmVimInfo'
                    ).find(
                        '{http://www.vmware.com/vcloud/extension/v1.5}VmVimObjectRef'
                    ).find(
                        '{http://www.vmware.com/vcloud/extension/v1.5}MoRef'
                    ).text
                    self.logger.info('Dispatching to reactioneer vm_mo_ref: {}'.format(vm_mo_ref))
                    self.reaction_queue.put(vm_mo_ref)
                else:
                    self.logger.warning('Entity: {urn} has failed to be created. Skipping it.'.format(urn=entity))
                resolver_queue.task_done()
            else:
                # Session lost to vcloud... Relogging in...
                self.logger.debug("Probably session lost to vcloud. Reconnecting.")
                self.login()
                resolver_queue.put(entity)
                resolver_queue.task_done()


def callback(ch, method, properties, body):
    xml_msg = ET.fromstring(body)
    if xml_msg.attrib['type'] == 'com/vmware/vcloud/event/vm/create':
        entity_links = xml_msg.findall('{http://www.vmware.com/vcloud/extension/v1.5}EntityLink')
        entity_id = filter(lambda entity: entity.attrib['type'] == 'vcloud:vm', entity_links)[0].attrib['id']
        resolver_queue.put(entity_id)


class Watcher2(Thread):
    """
    Base worker to abstract all connection and processing
    logic away for simplification
    """
    def __init__(self, rabbitmq, username, password, queue='shepherd', durable=True):
        """
        Construct the worker
        """
        # Call super process init
        super(Watcher2, self).__init__()
        # Set the process name
        self.name = 'Watcher2'

        self.channel = None
        self.connection = None
        self.username = username
        self.password = password
        self.queue = queue
        self.is_queue_durable = durable

        self.parameters = pika.ConnectionParameters(
            host=rabbitmq,
            credentials=pika.PlainCredentials(username=self.username, password=self.password)
        )

        self.logger = logging.getLogger('shepherd.watcher.Watcher2')
        self.logger.debug(str(self) + ' - Created')
        self.connect()

    def run(self):
        """
        Implement process start method, kicks off message
        processing in the ioloop
        """
        self.logger.info('Waiting for messages...')
        try:
            # Loop so we can communicate with RabbitMQ
            self.connection.ioloop.start()
        except Exception:
            # Don't care
            pass

    def stop(self):
        """
        Provide a way to stop this process and message processing
        """
        self.logger.debug('Stopping')
        try:
            # Gracefully close the connection
            self.channel.stop_consuming()
            self.connection.close()
            # Loop until we're fully closed, will stop on its own
            self.connection.ioloop.start()
        except Exception:
            # Don't care
            pass

    def connect(self):
        """
        Make a connection to the queue or exit
        """
        attempt = 1
        while attempt < 3:
            try:
                self.logger.debug(
                    str(self) + ' - Connecting - Attempt ' + str(attempt))

                self.connection = pika.SelectConnection(
                    self.parameters,
                    self.on_connected
                )
                self.logger.debug(str(self) + ' - Connected!')
                # Stop trying
                return

            except Exception:
                # Sleep then try again
                time.sleep(60)
            attempt += 1
        self.logger.error(str(self) + ' - Failed to connect, max attempts reached')
        # Kill the worker

    def on_connected(self, new_connection):
        """
        Callback for when a connection is made
        """
        # Save new connection
        self.connection = new_connection
        # Add callbacks
        self.connection.channel(self.on_channel_open)
        self.connection.add_on_close_callback(self.on_connection_closed)

    def on_channel_open(self, new_channel):
        """
        Callback for when a channel is opened
        """
        # Save new channel
        self.channel = new_channel
        # Setup channel
        self.channel.queue_declare(
            queue=self.queue,
            durable=self.is_queue_durable,
            callback=self.on_queue_declared,
        )

    def on_queue_declared(self, frame):
        """
        Callback for when a queue is declared
        """
        # Declare callback for consuming from queue
        self.channel.basic_consume(self.on_message, queue=self.queue)

        # Set additional options on queue: 1 msg at a time
        self.channel.basic_qos(prefetch_count=1)

    def on_message(self, channel, method, header, body):
        """
        Callback for when a message is received
        """

        # Break when num retries is reached
        if ('retries' in body and
                body['retries'] == 3):
            self.logger.error(str(self) + " - Max retries failed...")

            # ACK - Finished with message, fail or retry
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            # Dispatch the processing of the message
            self.process(body)
        except ParseError:
            self.logger.error('Cannot parse message. Removing it.')
            pass
        except Exception as e:
            self.logger.error(e)
            self.logger.error(str(self) + " - Retrying...")

            # Upset retries
            if 'retries' not in body:
                body['retries'] = 1
            else:
                retries = body['retries']
                body['retries'] = retries + 1

        # Always ack, finished with message, fail or retry
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def process(self, message):
        """
        Method called to do something with the received message,
        to be implemented by extending classes
        """
        xml_msg = ET.fromstring(message)
        if xml_msg.attrib['type'] == 'com/vmware/vcloud/event/vm/create':
            self.logger.info('Received a message containing a VM Create Action. Processing...')
            entity_links = xml_msg.findall('{http://www.vmware.com/vcloud/extension/v1.5}EntityLink')
            entity_id = filter(lambda entity: entity.attrib['type'] == 'vcloud:vm', entity_links)[0].attrib['id']
            resolver_queue.put(entity_id)
            self.logger.info("Entity ID: {} dispatched to resolver.".format(entity_id))

    def on_connection_closed(self, frame):
        """
        Callback for when a connection is closed
        """
        self.logger.error(str(self) + ' - Connection lost!')
        self.stop()
        self.connect()
        self.run()

    def __repr__(self):
        """
        String repr for a Worker
        """
        return self.name
