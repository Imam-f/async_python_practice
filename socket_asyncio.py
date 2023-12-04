import asyncio

async def handle_client(reader, writer):
    # Get the client IP address
    client_ip = writer.get_extra_info('peername')[0]
    print(f"Client connected from {client_ip}")
    
    # Handle client requests
    while True:
        data = await reader.read(1024)
        if not data:
            break
        response = "Received: " + data.decode()
        writer.write(response.encode())
        await writer.drain()
    writer.close()

async def start_server():
    # Create a socket server
    server = await asyncio.start_server(
        handle_client, 'localhost', 12345)
    
    # Get the server socket object
    addr = server.sockets[0].getsockname()
    print(f'Server listening on {addr}')
    
    # Serve incoming connections
    async with server:
        await server.serve_forever()

async def render():
    # Run the rendering function concurrently
    while True:
        # Perform rendering tasks
        await asyncio.sleep(1)  # Simulating rendering work
        print("Rendering...")

# Create the event loop
loop = asyncio.get_event_loop()

# Create tasks for the server and rendering functions
server_task = loop.create_task(start_server())
render_task = loop.create_task(render())

# Run the event loop
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    # Cancel tasks and stop the event loop
    server_task.cancel()
    render_task.cancel()
    loop.run_until_complete(asyncio.gather(server_task, render_task))
    loop.close()