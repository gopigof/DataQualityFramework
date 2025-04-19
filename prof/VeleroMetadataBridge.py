try:
    import ServerConfig
except ImportError as e:
        print(f"Failed to import ServerConfig module: {e}")
else:
    config = ServerConfig.read_config ()
    serverinfo = config["DestinationServer"]
    __database = serverinfo["__database"]
    __username = serverinfo["__username"]
    __password = serverinfo["__password"]
    __server = serverinfo["__server"]
    __port = serverinfo["__port"]
    if None in [__database, __username, __password, __server, __port]:
        raise ValueError("One or more required server configuration values are missing.")
    # print ( 'database is: ', __database, ' Server: ', __server, ' port: ', __port, ' username: ', __username, ' password: ', __password)
