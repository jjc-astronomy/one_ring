import socket

def find_free_ports(num_ports):
    ports = []
    sockets = []
    for _ in range(num_ports):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', 0))
        # get the allocated port number
        addr, port = s.getsockname()
        ports.append(port)
        sockets.append(s)
    # keep sockets open until they are needed
    for s in sockets:
        s.close()
    return ports

# Find 5 available ports
free_ports = find_free_ports(5)
print("Allocated ports:", free_ports)
