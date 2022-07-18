import http.server
import socketserver
from http.server import HTTPServer, SimpleHTTPRequestHandler

PORT = 8000
DATA_DIR = "./data/download"


class DataHttpRequestHandler(SimpleHTTPRequestHandler):
    # def __init__(self,   request, client_address, server):
    #     SimpleHTTPRequestHandler.__init__(
    #         self, request, client_address, server)
    #     self.data_dir = DATA_DIR

    def build_basedir(self, api):
        return DATA_DIR + '/' + api

    def build_path(self, api, tconst):
        basedir = self.build_basedir(api)
        key = format(int(tconst[2:]), '08d')
        # reverse the id to obtain a partitionable key
        tpath = key[6:8]+'/'+key[4:6]+'/'+key[2:4]
        return basedir+'/'+tpath

    def do_GET(self):
        print('request for {}'.format(self.path))

        reqpath = self.path[1:].split('/', 1)
        if not len(reqpath) == 2:
            self.send_error(404)

        # fetch api path
        api = reqpath[0]
        tconst = reqpath[1]
        print('read api {} tconst {}'.format(api, tconst))

        filepath = self.build_path(api, tconst) + '/'+tconst+'.json'
        print('fetch file from {}'.format(filepath))
        self.path = filepath

        return SimpleHTTPRequestHandler.do_GET(self)


# run server on port
handler = DataHttpRequestHandler
server_address = ('', PORT)
httpd = HTTPServer(server_address, handler)
httpd.serve_forever()
