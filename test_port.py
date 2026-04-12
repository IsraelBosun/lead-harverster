import socket

try:
    s = socket.create_connection(('gmail-smtp-in.l.google.com', 25), timeout=5)
    s.close()
    print("Port 25 is OPEN")
except Exception as e:
    print(f"Port 25 BLOCKED: {e}")