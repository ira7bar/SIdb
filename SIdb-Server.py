import socket
import threading
import json
import time
import ruamel.yaml as yaml



OK = "OK"
GOODBYE = "goodbye"
OKGOODBYE = "ok_goodbye"

BACKUP = 'backup.txt'
BACKUPTIME = 30

class View(object):
    def __init__(self,msg=None):
        print(msg)

class Controller(object):
    def user_in(self,msg):
        return raw_input(msg)

class SIdb(object):
    def __init__(self):
        # self._database = {}
        # self._database_expires = {}
        self._get_yaml()
        self._read_from_disk()
        self._controller = Controller()
        self._server_ip = '127.0.0.1'
        # self._listening_ip = '0.0.0.0'
        # self._server_port = 3030
        # self._server_addr = ('0.0.0.0',3030)
        # self._server_recv_addr = ('10.35.77.231', 3030)
        # self._server_send_addr = ('10.35.77.231', 3031)


        self._server_recv_addr = ('0.0.0.0', self._in_port)
        self._server_send_addr = ('0.0.0.0', self._out_port)
        self._server_recv_socket = None
        self._server_send_socket = None
        self._clients = {}
        self._keep_accepting_connections = True
        self._check_ttl()
        self._initial_connection()
        self._connect_to_clients()
        self._close_server()

    def _get_yaml(self):
        with open('config_yaml.yml', 'r') as open_file:
            config_yaml_encoded = open_file.read()
            config_decoded = yaml.load(config_yaml_encoded,Loader=yaml.Loader)
            self._in_port = int(config_decoded['input_port'])
            self._out_port = int(config_decoded['output_port'])
            self._ttl_time = config_decoded['ttl_time']

    def _initial_connection(self):
        self._server_recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_recv_socket.bind(self._server_recv_addr)
        # avoid TIME_WAIT after closing
        self._server_recv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_recv_socket.listen(5)
        View('Server is on and receiving on {}:{}'.format(*self._server_recv_addr))

        self._server_send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_send_socket.bind(self._server_send_addr)
        # avoid TIME_WAIT after closing
        self._server_send_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_send_socket.listen(5)
        View('Server is on and sending on {}:{}'.format(*self._server_send_addr))

    def _client_send_connection(self,client_socket,client_address):
        client_ip = client_address[0]
        while client_ip not in self._clients:
            pass

        while self._clients[client_ip]['send_command'] != OKGOODBYE and self._clients[client_ip]['send_command'] != GOODBYE:
            if self._clients[client_ip]['send_command']:
                client_socket.sendall(self._clients[client_ip]['send_command'])
                View('Sending {}'.format(self._clients[client_ip]['send_command']))
                self._clients[client_ip]['send_command'] = None
            else:
                continue




        if self._clients[client_ip]['send_command'] == OKGOODBYE:
            View('Sending {}'.format(self._clients[client_ip]['send_command']))
            client_socket.sendall(OKGOODBYE)
            self._clients[client_ip]['send_command'] = None
            self._clients.pop(client_ip)
        elif self._clients[client_ip]['send_command'] == GOODBYE:
            View('Sending {}'.format(self._clients[client_ip]['send_command']))
            client_socket.sendall(GOODBYE)
            self._clients[client_ip]['send_command'] = None
        client_socket.close()


    def _client_recv_connection(self,client_socket,client_address):
        client_ip = client_address[0]
        ID = client_socket.recv(4096)

        # if ID in self._clients:
        #     View('Client {}:{} chose taken ID: {}'.format(client_address[0], client_address[1], ID))
        #     client_socket.sendall("Error - ID taken")
        #     return



        self._clients[client_ip] = {}

        self._clients[client_ip]['ID'] = ID

        self._clients[client_ip]['send_command'] = OK

        self._clients[client_ip]['recv_command'] = client_socket.recv(4096)
        command_str = self._clients[client_ip]['recv_command']

        while command_str != GOODBYE and command_str != OKGOODBYE:


            split_command = command_str.split(' ')

            if split_command[0].lower() == 'set':
                key = split_command[1]
                value = split_command[2]
                View('recieved command:\n{} {} {}'.format(split_command[0],key,value))
                self._insert_data(key,value)
            elif split_command[0].lower() == 'get':
                key = split_command[1]
                View('recieved command:\n{} {}'.format(split_command[0], key))
                value = self._get_data(key)
                self._clients[client_ip]['send_command'] = value
            elif split_command[0].lower() == 'prefix':
                prefix = split_command[1]
                View('recieved command:\n{} {}'.format(split_command[0], prefix))
                dict = self._get_prefix(prefix)
                dict_json = json.dumps(dict)
                self._clients[client_ip]['send_command'] = dict_json

            else:
                View('recieved unknown command')
                self._clients[client_ip]['send_command'] = 'unknown command!'


            self._clients[client_ip]['recv_command'] = client_socket.recv(4096)
            command_str = self._clients[client_ip]['recv_command']

        if command_str == GOODBYE:
            self._clients[client_ip]['send_command'] = OKGOODBYE
        elif command_str == OKGOODBYE:
            pass
            # self._clients.pop(client_ip)

        client_socket.close()
        View('Client ID {} disconnected'.format(ID))


    def _accept_send_connections(self):
        while self._keep_accepting_connections:
            client_send_socket, client_send_address = self._server_send_socket.accept()
            View('Accepted send connection from {}:{}'.format(*client_send_address))

            if self._keep_accepting_connections == False:
                break

            client_send_thread = threading.Thread(
                target = self._client_send_connection,
                args = (client_send_socket,client_send_address))
            client_send_thread.start()

        # self._close_server()


    def _accept_recv_connections(self):
        while self._keep_accepting_connections:
            client_recv_socket, client_recv_address = self._server_recv_socket.accept()
            View('Accepted recv connection from {}:{}'.format(*client_recv_address))

            if self._keep_accepting_connections == False:
                break

            client_recv_thread = threading.Thread(
                target = self._client_recv_connection,
                args = (client_recv_socket,client_recv_address))
            client_recv_thread.start()


    def _close_accepting_thread(self):
        self._keep_accepting_connections = False
        View('keep accepting thread = {}'.format(self._keep_accepting_connections))

        if self._accepting_recv_thread.is_alive():
            # create local connection to trigger accept:
            s = socket.socket()
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.connect((self._server_ip,self._server_recv_addr[1]))
            s.close()

        if self._accepting_send_thread.is_alive():
            # create local connection to trigger accept:
            s = socket.socket()
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.connect((self._server_ip, self._server_send_addr[1]))
            s.close()




    def _connect_to_clients(self):


        self._accepting_recv_thread = threading.Thread(target=self._accept_recv_connections)
        self._accepting_recv_thread.start()

        self._accepting_send_thread = threading.Thread(target=self._accept_send_connections)
        self._accepting_send_thread.start()

        while True:
            View('Press l for client list, d ID to close client, f to flush to HD, c to clean DB, x to exit, else to refresh')
            c = self._controller.user_in('> ')
            if c == 'x':
                break
            if c == 'l':
                for client in self._clients.keys():
                    View('{}'.format(self._clients[client]['ID']))
            if c == 'f':
                self._dump_to_disk()
            if c == 'c':
                self._clean_DB()
            if c.startswith('d'):
                ID = c.split(' ')[1]
                for client_ip in self._clients.keys():
                    if self._clients[client_ip]['ID'] == ID:
                        self._clients[client_ip]['send_command'] = GOODBYE
            else:
                continue

        for client_ip in self._clients.keys():
            self._clients[client_ip]['send_command'] = GOODBYE
         #   send goodbye to all clients

        View('Stopping listening for new connections')
        self._close_accepting_thread()


        # waiting for accepting thread to close
        self._accepting_send_thread.join()
        self._accepting_recv_thread.join()

        View('All clients disconnected')

    def _ttl_thread(self):

        start_time = time.time()

        while self._keep_accepting_connections:
            for key in self._database_expires.keys():

                if self._database_expires[key] < time.time():
                    View('{} expired, popping'.format(key))
                    self._database_expires.pop(key)
                    self._database.pop(key)

            if time.time() - start_time > BACKUPTIME:
                self._dump_to_disk()
                start_time = time.time()




    def _check_ttl(self):
        ttl_thread = threading.Thread(target=self._ttl_thread)
        ttl_thread.start()

    def _clean_DB(self):
        for k in self._database.keys():
            self._database.pop(k)

    def _dump_to_disk(self):
        database_json = json.dumps(self._database)
        with open(BACKUP, 'w') as f:
            f.write(database_json)

    def _read_from_disk(self):
        with open(BACKUP,'r') as f:
            d = f.read()
        self._database = json.loads(d)
        self._database_expires = {}
        for k in self._database.keys():
            self._database_expires[k] = time.time() + self._ttl_time



    def _close_server(self):
        self._server_send_socket.close()
        self._server_recv_socket.close()


    def _insert_data(self,key,value):
        if key in self._database:
            View('Key {} exists, overriding value with {}'.format(key,value))
        else:
            View('{} {} inserted to database'.format(key,value))
        self._database[key] = value
        self._database_expires[key] = time.time() + self._ttl_time


    def _get_data(self,key):
        if key in self._database:
            return self._database[key]
        else:
            View('Error - key {} not found'.format(key))
            return "Error - key {} not found".format(key)

    def _get_prefix(self,key):
        dict = {}
        for k in self._database.keys():
            if k.startswith(key):
                dict[k] = self._database[k]
        return dict


def main():

    s = SIdb()

if __name__ == '__main__':
    main()