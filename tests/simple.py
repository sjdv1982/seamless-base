import seamless
from seamless import Buffer

b = Buffer(b"a default buffer")
print(b.get_checksum())

import asyncio

asyncio.run(b.write())  # no-op

b2 = Buffer(b"a default buffer 2")
print(b2.get_checksum())
asyncio.run(b2.write())  # no-op

c = b.get_checksum()
c2 = b2.get_checksum()

b.incref()
b2.incref()
del b, b2

print(c.resolve(), c2.resolve())
