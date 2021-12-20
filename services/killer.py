"""
This module is used to kill nodes when certain criteria is met.
The rules to kill nodes are defined in JSON files which are formed by
the node worker name/type and it's ID. For example, to set a rule for
a joiner worker with ID 2 you'd create the file ./killer_conf/join_2.json

Inside the workers there will be "stages" to trigger the rules. These stages
are defined by calling the `kill_if_applies` function defined in this module.

For example, we can define an "after_msg" stage to kill a node after a specific
number of messages were received by calling:

    >>> kill_if_applies(stage='after_msg', msg_count=n, correlation_id=cid)

Inside the configuration file, there should be JSON objects like the following:

    {
        "kill on msg 12 for cid 1234": {
            "stage": "after_msg",
            "params": {
                "msg_count": 12,
                "correlation_id": "1234"
            }
        },
        "foo": {
            "stage": "after_msg",
            "params": {
                "msg_count": 42
                "correlation_id": "9999999"
            }
        },
    }

The key ("kill on msg 12 for cid 1234") indicates the name of the rule. Inside the rule
there is the "stage" when the rule is to be applied (see the call to `kill_if_applies` above)
and the "params" fields should match the ones passed to `kill_if_applies` after the stage.
These fields will be compared against the ones defined in the configuration
and, if they match, the program exist. In the example, `n` should be 12 and `cid` should be
"1234" in order for the first rule to trigger.

Once a rule is executed it is deleted from the file (so we don't end up in an infinite loop
of shutdowns). You can set several rules on the same node to cause multiple shutdowns.
"""

import os
import json

from collections import namedtuple


WORKER_ID = os.environ['WORKER_ID']
WORKER_TASK = os.environ['WORKER_TASK']

# path to the configuration file for this node based on the location
# inside Docker. On the host, the file should be under the ./killer_conf/
# directory (at the root of the repository)
CONF_FILE = f'/app/killer_conf/{WORKER_TASK}_{WORKER_ID}.json'


KillRule = namedtuple('KillRule', 'name, stage, params')


def _load_conf():
    try:
        with open(CONF_FILE) as fp:
            conf = json.load(fp)

        rules = [KillRule(name, **rule) for name, rule in conf.items()]
        return rules
    except FileNotFoundError:
        return []


RULES = _load_conf()


def kill_if_applies(stage:str, **params):
    for rule in RULES:
        if rule.stage != stage:
            continue

        for param_name, expected_value in rule.params.items():
            if params[param_name] != expected_value:
                break
        else:
            # if all params match, then we kill the program
            print('[ KILLER ] killing node by rule', rule.name)
            _delete_rule(rule)
            exit(2)


def _delete_rule(rule:KillRule):
    with open(CONF_FILE) as fp:
        conf = json.load(fp)

    del conf[rule.name]

    with open(CONF_FILE, 'w') as fp:
        json.dump(conf, fp, indent=4)
