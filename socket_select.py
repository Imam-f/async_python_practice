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
    
    # List to keep track of connected clients
    clients = [server_socket]
    
    while True:
        # Use select to monitor sockets for activity
        read_sockets, _, _ = select.select(clients, [], [])
        
        for sock in read_sockets:
            if sock == server_socket:
                # Handle new client connections
                client_socket, client_address = server_socket.accept()
                print(f"New connection from {client_address}")
                clients.append(client_socket)
            else:
                # Handle incoming data from clients
                data = sock.recv(1024)
                if data:
                    response = "Received: " + data.decode()
                    sock.send(response.encode())
                else:
                    # Close the client connection if no data is received
                    print(f"Connection closed by {client_address}")
                    clients.remove(sock)
                    sock.close()

# Start the server
start_server()