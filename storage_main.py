from babel_library.librarian import Librarian
#from commons import liveness_agent
from services import liveness_agent



liveness_agent.start_server_in_new_thread()
lib = Librarian()
