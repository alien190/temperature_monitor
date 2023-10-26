import logging

class Logger():
    def __init__(self, filename, is_debug_enabled: bool):
        self.is_debug_enabled = is_debug_enabled
        logging.basicConfig(level=logging.INFO,filename=filename,filemode="a", format="%(asctime)s %(levelname)s %(message)s")

    def log_info(self, msg):
        if(self.is_debug_enabled):
            print(msg)
            logging.info(msg)
     
    def log_error(self, msg):
        print('Error:' + msg)
        logging.error(msg)