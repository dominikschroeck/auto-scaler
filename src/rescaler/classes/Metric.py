class Metric:

    __name = ""
    __value = 0.0
    __list = []
    __ma = 0.0


    def get_value(self):
        return self.__value

    def get_name(self):
        return self.__name

    def set_list(self,lis):
        self.__list += lis

    def get_list(self):
        return self.__list

    def set_value(self,value):
        self.__value = value

    def __init__(self, name,value):
        self.__value = value
        self.__name = name
        self.__ma = 0.0
        self.__list = []

    def set_max(self, ma):
        self.__ma = ma

    def get_max(self):
        return self.__ma


