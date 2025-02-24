import os
from py_clob_client.constants import POLYGON
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY

host = "https://clob.polymarket.com"
key = os.getenv("PK")
chain_id = POLYGON

# Create CLOB client and get/set API credentials
client = ClobClient(host, key=key, chain_id=chain_id)
client.set_api_creds(client.create_or_derive_api_creds())

# Create and sign an order buying 100 YES tokens for 0.50c each
resp = client.create_and_post_order(OrderArgs(
    price=0.50,
    size=100.0,
    side=BUY,
    token_id="71321045679252212594626385532706912750332728571942532289631379312455583992563"
))

print(resp)