if __name__ == '__main__':
    # Test AMBERServer
    manager = AMBERServer(address=("", PORT_AMBER), authkey=AUTH_AMBER)
    server = manager.get_server()
    server.serve_forever()
