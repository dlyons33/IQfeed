import configparser

if __name__ == "__main__":
	config = configparser.ConfigParser()
    config.read('config.ini')

    pwd = configparser.ConfigParser()
    pwd.read('user.pwd')