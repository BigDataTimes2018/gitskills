# -*- coding: utf-8 -*-
import logging
import ctypes
FOREGROUND_WHITE = 0x0007
FOREGROUND_BLUE = 0x01  # text color contains blue.
FOREGROUND_GREEN = 0x02  # text color contains green.
FOREGROUND_RED = 0x04  # text color contains red.
FOREGROUND_YELLOW = FOREGROUND_RED | FOREGROUND_GREEN
STD_OUTPUT_HANDLE = -11
std_out_handle = ctypes.windll.kernel32.GetStdHandle(STD_OUTPUT_HANDLE)

def set_color(color, handle=std_out_handle):
    bool = ctypes.windll.kernel32.SetConsoleTextAttribute(handle, color)
    return bool
# 因为多个文件都需要用到这个日志信息，这里把它单独的封装一个类，然后在每个文件中得到这个实例就可以了
class Logger:
    def __init__(self,path,clevel = logging.DEBUG,Flevel = logging.DEBUG):
        self.logger = logging.getLogger(path)
        self.logger.setLevel(logging.DEBUG)
        # cmd打印的展示的日志信息，设置了日志的格式
        formatter=logging.Formatter('%(asctime)s %(name)s[line:%(lineno)d] %(levelname)s %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        ch.setLevel(clevel)
       #设置文件的日志格式
        fh = logging.FileHandler(path)
        fh.setFormatter(formatter)
        fh.setLevel(Flevel)
        # 将hander添加到logger对象中
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def war(self, message):
        self.logger.warn(message)
        set_color(FOREGROUND_WHITE)

    def error(self, message):
        self.logger.error(message)
        set_color(FOREGROUND_WHITE)

    def cri(self, message):
        self.logger.critical(message)

