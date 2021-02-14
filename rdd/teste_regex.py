import re
s = "avós surubão sofá" # Sample string 
out = re.sub(r'[^a-zA-Z0-9\s]', '', s)
print(out)
	