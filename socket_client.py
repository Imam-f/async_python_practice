import socket

def echo_client():
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect the socket to the server
    server_address = ('localhost', 12345)
    print('connecting to {} port {}'.format(*server_address))
    sock.connect(server_address)

    try:
        # Send data
        message = 'This is the message to be echoed'
        print('sending {!r}'.format(message))
        sock.sendall(message.encode('utf-8'))

        # Receive data from the server
        data = sock.recv(1024)
        print("Received:", data.decode())

        sock.close()
    except:
        print("Shippai")

echo_client()
