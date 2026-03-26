# tests/test_ping.py
import socket

def send_command(sock, cmd):
    sock.sendall((cmd + "\n").encode())
    response = b""
    while True:
        chunk = sock.recv(4096)
        response += chunk
        if b"END\n" in response:
            break
    return response.decode()

def test_ping():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", 5433))

    response = send_command(sock, "PING")
    print("Response:", repr(response))

    assert "OK\n"   in response, "missing OK"
    assert "PONG\n" in response, "missing PONG"
    assert "METRICS" in response, "missing METRICS"
    assert "END\n"  in response, "missing END"

    print("PASS: test_ping")
    sock.close()

if __name__ == "__main__":
    test_ping()
