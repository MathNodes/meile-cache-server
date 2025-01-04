
from urllib.parse import urlparse

URL = "https://scheolde8250.scheolde.one:443"

parsedURL = urlparse(URL).netloc.split(":")

print(parsedURL)