import json
from service_config import LIBRARIANS, BULLY_TIMEOUT, NUMBER_OF_MONITOR_CONTAINERS, WORKERS

def get_monitor_nodes():

    nodes = {}

    for monitmonitor_number in range(1, NUMBER_OF_MONITOR_CONTAINERS+1):
        nodes[monitmonitor_number] = f'http://monitor_{monitmonitor_number}:808{monitmonitor_number}'

    return nodes    

def get_system_nodes():

    systemNodes = {}

    for monitmonitor_number in range(1, NUMBER_OF_MONITOR_CONTAINERS+1):
        systemNodes[f'monitor_{monitmonitor_number}'] = f'http://monitor_{monitmonitor_number}:808{monitmonitor_number}'

    for librarian in LIBRARIANS:
        name = librarian['name']
        port = librarian['port']
        systemNodes[name] = f'http://{name}:80'

    for worker_type_name, replicas in WORKERS.items():
        for n in range(replicas):
            name = f'{worker_type_name}_{n}'
            systemNodes[name] = f'http://{name}:80'

    return systemNodes


def create_monitor_config():

    nodes = get_monitor_nodes()
    systemNodes = get_system_nodes()

    for monitor_number in range(1, NUMBER_OF_MONITOR_CONTAINERS+1):
        config = {}
        config['id'] = monitor_number
        config['nodes'] = nodes
        #sacarme de system nodes
        systemNodes_ = systemNodes.copy()
        systemNodes_.pop(f'monitor_{monitor_number}')
        config['systemNodes'] = systemNodes_
        config['timeout'] = BULLY_TIMEOUT #seg
        config['addr'] = f'monitor_{monitor_number}'
        config['port'] = int(f'808{monitor_number}')

        # Serializing json 
        json_object = json.dumps(config, indent = 4)
    
        # Writing to sample.json
        with open(f'./monitor/src/config{monitor_number}.json', "w") as outfile:
            outfile.write(json_object)

if __name__ == '__main__':
    import sys

    assert len(sys.argv) == 1, 'Usage  monitor_config_builder.py'

    create_monitor_config()
