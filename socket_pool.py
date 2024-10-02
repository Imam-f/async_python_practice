import socket
import select

def start_server():
    # Create a socket object
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Bind the socket to a specific address and port
    server_socket.bind(('localhost', 12345))
    
    # Set the socket to non-blocking mode
    server_socket.setblocking(False)
    
    # Listen for incoming connections
    server_socket.listen(5)
    print("Server listening on port 12345...")
    
    # Create a poll object
    poll_object = select.poll()
    
    # Register the server socket for monitoring
    poll_object.register(server_socket, select.POLLIN)
    
    # Dictionary to map file descriptors to client sockets
    clients = {server_socket.fileno(): server_socket}
    
    while True:
        # Use poll to monitor sockets for activity
        events = poll_object.poll()
        
        for fd, event in events:
            if fd == server_socket.fileno():
                # Handle new client connections
                client_socket, client_address = server_socket.accept()
                print(f"New connection from {client_address}")
                client_socket.setblocking(False)
                poll_object.register(client_socket, select.POLLIN)
                clients[client_socket.fileno()] = client_socket
            elif event & select.POLLIN:
                # Handle incoming data from clients
                client_socket = clients[fd]
                data = client_socket.recv(1024)
                if data:
                    response = "Received: " + data.decode()
                    client_socket.send(response.encode())
                else:
                    # Close the client connection if no data is received
                    print(f"Connection closed by {client_address}")
                    poll_object.unregister(client_socket)
                    clients.pop(fd)
                    client_socket.close()

# Start the server
start_server()