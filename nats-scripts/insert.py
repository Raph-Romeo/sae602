import asyncio
from nats.aio.client import Client as NATS
import time
import json
import sys

# SCRIPT POUR L'INSERTION DE DONNEES

class Main:
    def __init__(self, address):
        self.nc = None
        self.server_address = address
        self.queue_group = "lb"
        self.topic_name = "sae/insert"

    async def run(self):
        self.nc = NATS()
        await self.nc.connect(servers=[self.server_address])
        await self.subscribe()
        while True:
            await asyncio.sleep(1)

    async def handle_message(self, msg):
        data = json.loads(msg.data.decode())
        action = data["action"]
        print(f"New message : {data}")
        if action == 1:  # Insertion d'argent
            account_id = data["account_id"]
            amount = data["amount"]
        elif action == 2:  # Creation de compte
            name = data["name"]
            amount = 0
        # SIMULATE WORKLOAD
        time.sleep(30)

    async def subscribe(self):
        await self.nc.subscribe(self.topic_name, queue=self.queue_group, cb=self.handle_message)
        print(f"Subscribed to TOPIC : {self.topic_name}")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    if len(sys.argv) == 1:
        server_address = "nats://192.168.1.54:4222"
    else:
        server_address = sys.argv[1]
    main = Main(server_address)
    loop.run_until_complete(main.run())
