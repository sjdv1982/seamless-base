from seamless import Buffer

b = Buffer(b"a default buffer")
print(b.get_checksum())

b2 = Buffer(b"a default buffer 2")
print(b2.get_checksum())
