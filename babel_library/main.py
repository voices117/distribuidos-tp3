from librarian import Librarian
from commons import liveness_agent
#from services import liveness_agent
# import sys

# sys.path.insert(0, '..')


liveness_agent.start_server_in_new_thread()
lib = Librarian()
