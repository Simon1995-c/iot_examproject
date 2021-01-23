import asyncio
from mqtt_broker import Broker

def main():
    loop = asyncio.get_event_loop()
    broker = Broker()
    loop.run_until_complete(broker.start())
    loop.run_forever()


if __name__ == '__main__':
    main()