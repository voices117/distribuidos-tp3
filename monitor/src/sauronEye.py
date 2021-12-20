import json
import logging
import subprocess
import time
import requests
from signal import signal, SIGTERM

from bullyAlgorithm import Bully

OK = 0

class sauronEye:
    
    def __init__(self) -> None:
        with open('/config.json') as json_data_file:
            data = json.load(json_data_file)
            self.Bully = Bully(data)
            self.Nodes = data['systemNodes']
            self.timeout = data['timeout']
            print(data)
        self.running = True    
        self.Bully.start()
        logging.basicConfig(level=logging.INFO)
        signal(SIGTERM, self.__handler)

    def _bringBackToLife(self, containerName):
        
        result = subprocess.run(['docker', 'start', containerName], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))    
        
        return result.returncode

    def __handler(self, signal_received, frame):
        # SIGTERM handler
        logging.info('SIGTERM detected. Exiting gracefully')
        self.Bully.close()
        self.Bully.join()
        self.running = False

    def loop(self):
        logging.info('Checking nodes status')
        for container, addr in self.Nodes.items():
            if not self.running or not self.Bully.amICoordinator():
                # someone killed me or revived the leader
                return
            try:
                response = requests.get(url=addr + '/status', timeout=self.timeout)
                if response.status_code != requests.codes.ok:
                    if self._bringBackToLife(container) == OK:
                        logging.info(f'node {container} has been restored')
                    else:
                        logging.info(f'an error occurred trying to restore {container}') 
            except:
                if self._bringBackToLife(container) == OK:
                    logging.info(f'node {container} has been restored')
                else:
                    logging.info(f'an error occurred trying to restore {container}')    

    def run(self):
        while self.running:
            if not self.Bully.amICoordinator():
                logging.info('slave node')
            else:
                self.loop()
            time.sleep(10) 
        logging.info('bye bye sauron')

s = sauronEye()
s.run()